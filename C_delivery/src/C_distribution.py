"""Training-only conditional empirical survival models for arbitrary rainfall thresholds."""
from bisect import bisect_right
from collections import defaultdict
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from c_common import NUMERIC_FEATURES, KEYS, dump, load, sha256

CALENDAR=['hour_sin','hour_cos','month_sin','month_cos']
PERSISTENCE=['wet_slots_60m','wet_current','wet_lag_5m','wet_lag_10m','consecutive_wet_slots_60m',
             'wet_run_left_censored_60m','minutes_since_last_rain_60m_capped','no_rain_observed_60m']
RECENT=[x for x in NUMERIC_FEATURES if x not in CALENDAR+PERSISTENCE]

def validated_threshold(value):
    if isinstance(value,bool):raise ValueError('Threshold must be finite and nonnegative')
    try:d=Decimal(str(value))
    except InvalidOperation as exc:raise ValueError('Invalid threshold') from exc
    if not d.is_finite() or d<0 or d>=Decimal('1e20'):raise ValueError('Threshold must be finite, nonnegative and fit Decimal(38,18)')
    with localcontext() as context:
        context.prec=50
        if d!=d.quantize(Decimal('1e-18')):raise ValueError('Threshold exceeds the supported exact decimal scale (18)')
    return d

def survival(distribution,threshold):
    """Strict > survival; right bisect makes equality a non-event."""
    values=[Decimal(v) for v in distribution['values']]
    index=bisect_right(values,validated_threshold(threshold))
    return 1-(distribution['cum_counts'][index-1]/distribution['n'] if index else 0)

def conditional_survival(cdf,key,threshold):
    prior=survival(cdf['global'],threshold);cell=cdf['groups'].get(str(key))
    if cell is None:return prior
    alpha=cdf['prior_anchors'];return (cell['n']*survival(cell,threshold)+alpha*prior)/(cell['n']+alpha)

def enrich(frame):
    from pyspark.sql import functions as F
    stamp=F.from_unixtime('prediction_epoch_s').cast('timestamp')
    return frame.withColumn('prediction_month',F.month(stamp)).withColumn('prediction_hour',F.hour(stamp))

def grouping(frame,kind):
    from pyspark.sql import functions as F
    frame=enrich(frame)
    if kind=='climatology':key=F.concat_ws('|','station_id',F.col('prediction_month').cast('string'),F.col('prediction_hour').cast('string'))
    elif kind=='persistence':
        r=F.col('rain_sum_15m_mm');bin=F.lit(6)
        for i,cut in reversed(list(enumerate([0,.1,.5,2,5,10]))):bin=F.when(r<=cut,F.lit(i)).otherwise(bin)
        key=F.concat_ws('|',bin.cast('string'),(F.col('current_5m_mm')>0).cast('int').cast('string'))
    elif kind=='frequency':key=F.lit('global')
    else:key=F.col('leaf_id').cast('int').cast('string')
    return frame.withColumn('cdf_group',key)

def features_for(name):
    if name=='cdf_recent_d8':return RECENT,False
    if name=='cdf_persistence_d8' or name.startswith('cdf_persistence_d8_leaf'):return RECENT+PERSISTENCE,False
    if name=='cdf_calendar_d8':return RECENT+PERSISTENCE+CALENDAR,False
    return RECENT+PERSISTENCE+CALENDAR,True

def fit_tree(train,name,config):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer,OneHotEncoder,VectorAssembler
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.sql import functions as F
    inputs,station=features_for(name);stages=[]
    if station:
        stages+=[StringIndexer(inputCol='station_id',outputCol='station_index',handleInvalid='error'),
                 OneHotEncoder(inputCols=['station_index'],outputCols=['station_ohe'],handleInvalid='error')]
    stages+=[VectorAssembler(inputCols=inputs+(['station_ohe'] if station else []),outputCol='features',handleInvalid='error'),
             DecisionTreeRegressor(labelCol='amount_log',predictionCol='amount_prediction',leafCol='leaf_id',
                 maxDepth=5 if name.endswith('d5') else 8,maxBins=config['tree_cdf']['maxBins'],
                 minInstancesPerNode=config.get('candidate_overrides',{}).get(name,{}).get('minInstancesPerNode',config['tree_cdf']['minInstancesPerNode']),seed=config['seed'])]
    model=Pipeline(stages=stages).fit(train.withColumn('amount_log',F.log1p(F.col('future_30m_mm').cast('double'))))
    return model

def fit_cdf(train,kind,prior):
    from pyspark.sql import functions as F
    records=grouping(train,kind).groupBy('cdf_group','future_30m_mm').count().collect()
    groups=defaultdict(lambda:defaultdict(int));global_counts=defaultdict(int)
    for r in records:
        value=str(r.future_30m_mm);groups[r.cdf_group][value]+=r['count'];global_counts[value]+=r['count']
    def distribution(counts):
        keys=sorted(counts,key=Decimal);acc=0;cum=[]
        for k in keys:acc+=counts[k];cum.append(acc)
        return {'values':keys,'cum_counts':cum,'n':acc}
    return {'kind':kind,'prior_anchors':prior,'global':distribution(global_counts),
            'groups':{k:distribution(v) for k,v in groups.items()},'label_rule':'future_30m_mm > threshold_mm'}

def score_cdf(spark,anchors,thresholds,cdf,model=None,extra_columns=()):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType,StructField,StringType,DoubleType
    frame=grouping(model.transform(anchors) if model is not None else anchors,cdf['kind'])
    entries=[(key,float(t),conditional_survival(cdf,key,t)) for key in cdf['groups'] for t in thresholds]
    lookup=spark.createDataFrame(entries,StructType([StructField('cdf_group',StringType(),False),StructField('threshold_mm',DoubleType(),False),StructField('conditional_p',DoubleType(),False)]))
    expanded=frame.withColumn('threshold_mm',F.explode(F.array(*[F.lit(float(t)) for t in thresholds])))
    priors=F.create_map(*[v for t in thresholds for v in [F.lit(float(t)),F.lit(survival(cdf['global'],t))]])
    scored=expanded.join(F.broadcast(lookup),['cdf_group','threshold_mm'],'left').withColumn('p',F.coalesce('conditional_p',priors[F.col('threshold_mm')]))
    return scored.withColumn('label',(F.col('future_30m_mm')>F.col('threshold_mm').cast('decimal(38,18)')).cast('double')).select(*KEYS,'threshold_mm','label','p',*extra_columns)

class DistributionPredictor:
    def __init__(self,spark,artifact):
        from pyspark.ml import PipelineModel
        self.spark=spark;self.artifact=Path(artifact)
        manifest=load(self.artifact/'artifact_manifest.json')
        for item in manifest['files']:
            path=self.artifact/item['path']
            if not path.is_file() or path.stat().st_size!=item['bytes'] or sha256(path)!=item['sha256']:raise ValueError('Model artifact checksum differs: '+item['path'])
        self.contract=load(self.artifact/'prediction_contract.json')
        self.cdf=load(self.artifact/'conditional_cdf.json');self.model=PipelineModel.load(str(self.artifact/'spark_model')) if (self.artifact/'spark_model').exists() else None
    def predict(self,observations,station_id,prediction_epoch_s,threshold_mm):
        from c_common import features_at
        t=validated_threshold(threshold_mm)
        if station_id not in self.contract['station_ids']:raise ValueError('Station not supported by training')
        frame=features_at(observations,station_id,prediction_epoch_s).select(*KEYS,*NUMERIC_FEATURES)
        frame=grouping(self.model.transform(frame) if self.model else frame,self.cdf['kind']);row=frame.select('cdf_group').first()
        p=conditional_survival(self.cdf,row.cdf_group,t)
        return {'station_id':station_id,'prediction_epoch_s':int(prediction_epoch_s),'threshold_mm':float(t),'threshold_decimal_mm':str(t),'probability':p,
                'event':'future_30m_mm > threshold_mm','evaluation_scope':'Development validation at nine cuts in 0..5 mm; empirical higher tail not independently validated',
                'information_availability':'Retrospective event-time assumption; not verified live publication latency'}
