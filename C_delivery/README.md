# C: Spark MLlib rainfall exceedance model

Completed probability model for strict accumulated rainfall > input threshold over the next 30 minutes. Inputs are station, five-minute endpoint prediction time and threshold, using B's 26 past-only features. Pinned Python 3.11/Java 17/PySpark 3.5.7; install requirements-C.txt. Follow the project README for formal data, frozen runtime and full reproduction.

```bash
python C_delivery/src/C_test_contracts.py
python C_delivery/src/C_train.py --help
```

C_train.py handles training-only threshold support, 1% pilot, 10% deterministic anchor sampling, three expanding folds, selection lock and final refit/test. C_package_model.py persists/reloads the model; C_predict.py exposes RainfallPredictor.predict(observations, station_id, prediction_epoch_s, threshold_mm). C_make_report.py generates English evidence-based reports/figures; C_finalize.py records output inventory and packages the deployed model. Use new run/output paths and consult each --help rather than overwriting original receipts.

Results: results/C_v1_20261012. Selected depth-5 decision tree: development mean Brier 0.017585; final 2024 mean 0.017736 versus frequency baseline 0.028883 (38.59% skill). All five threshold metrics, reliability, coverage, joins, selection lock and timing are preserved in JSON/CSV. Final model is results/C_v1_20261012/trained_model/ and output/trained_model_C_v1.zip; source/model/PDF submission copies are under submission/.

```python
from C_predict import RainfallPredictor
predictor = RainfallPredictor(spark, model_path)
result = predictor.predict(observations, station_id, prediction_epoch_s, threshold_mm)
```

Supply only existing history if desired. The exact 12 endpoints t−3300,...,t must be valid. Reject unknown stations, off-grid times and thresholds outside 0/0.5/1/2/5 mm. Inference does not use future eligibility. Reload verification compared 100 anchors/500 probabilities with maximum difference zero; past-only predictions matched offline results. Official interval semantics/publication latency remain unverified assumptions. These five evaluated thresholds do not establish arbitrary continuous-threshold performance. See submission/Final_Report_EN.md for complete methods, limitations and AI use.
