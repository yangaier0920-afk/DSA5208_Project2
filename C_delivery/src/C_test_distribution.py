"""Boundary, shrinkage, threshold monotonicity and portability checks for C revision."""
from decimal import Decimal
from C_distribution import survival,conditional_survival,validated_threshold,features_for,RECENT,PERSISTENCE,CALENDAR
from C_finalize import dependency_versions

def main():
    d={'values':['0','0.500000000000000000','0.500000000000000001','2'],'cum_counts':[6,7,8,10],'n':10}
    assert survival(d,Decimal('0.5'))==.3 or abs(survival(d,Decimal('0.5'))-.3)<1e-15
    assert abs(survival(d,Decimal('0.500000000000000001'))-.2)<1e-15
    assert survival(d,2)==0 and survival(d,0)>0
    cdf={'global':d,'prior_anchors':10,'groups':{'a':{'values':['0','1'],'cum_counts':[1,2],'n':2}}}
    assert conditional_survival(cdf,'missing',.5)==survival(d,.5)
    assert abs(conditional_survival(cdf,'a',.5)-(1+10*.3)/12)<1e-15
    cuts=[0,.25,.5,.75,1,1.5,2,3,3.14159,5,1e10]
    pp=[conditional_survival(cdf,'a',v) for v in cuts];assert all(0<=p<=1 for p in pp) and all(a>=b for a,b in zip(pp,pp[1:]))
    for bad in [True,-.1,float('nan'),float('inf'),'1e-19','1e20']:
        try:validated_threshold(bad)
        except ValueError:pass
        else:raise AssertionError('Invalid threshold accepted: '+str(bad))
    for value in [Decimal('0.333333333333333333'),1e10,'3.14159']:validated_threshold(value)
    assert len(set(RECENT+PERSISTENCE+CALENDAR))==26
    a,_=features_for('cdf_calendar_d8');b,station=features_for('cdf_full_d8');assert a==b and station
    for candidate in ['cdf_persistence_d8_leaf250','cdf_persistence_d8_leaf1000']:
        values,station=features_for(candidate);assert values==RECENT+PERSISTENCE and not station
    assert 'pyspark==3.5.7' in dependency_versions()
    print('PASS: strict Decimal equality/1e-18 boundary, fallback/shrinkage, arbitrary thresholds, monotonicity, controlled feature order, portable dependency capture')
if __name__=='__main__':main()
