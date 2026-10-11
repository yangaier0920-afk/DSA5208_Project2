"""Portable five-case Spark checks for the anchor join guard."""
import argparse
from pathlib import Path
from b_common import PACKAGE,start,dump
parser=argparse.ArgumentParser()
parser.add_argument('--root',required=True,type=Path)
args=parser.parse_args()
from B_join_audit import audit_anchor_join
spark=start(args.root,'B join audit edge checks')
try:
 schema='station_id string,prediction_epoch_s long'
 a=spark.createDataFrame([('S1',300),('S1',600)],schema)
 cases=[('unique',a,a,'PASS'),('duplicate_left',a.union(a),a,'FAIL'),('duplicate_right',a,a.union(a),'FAIL'),
 ('missing_match',a,a.limit(1),'FAIL'),('null_key',spark.createDataFrame([(None,300)],schema),a,'FAIL')]
 evidence=[]
 for name,l,r,expected in cases:
  info=audit_anchor_join(l,r,scope=name);assert info['status']==expected,(name,info)
  evidence.append({'case':name,'expected_status':expected,'actual_status':info['status'],'test_status':'PASS'})
 dump(PACKAGE/'results/join_audit/synthetic_validation.json',{'status':'PASS','cases':evidence})
 print('Five join edge checks PASS')
finally:spark.stop()
