# DSA5208 Project 2: current integrated results and reproduction

This project provides an integrated English report, concise summary, complete source package and a trained continuous-threshold model. The historical classifier and its already-known 2024 test remain separate evidence.

## Results and evidence policy

The selected conditional-distribution tree uses 22 recent/persistence features. Development mean Brier is 0.01679503, versus stronger persistence baseline 0.01793895 (6.38% reduction) and original development tree 0.01758544 (4.40% reduction). Adding cyclic time/station coding did not improve this ablation. Twelve candidates have three-fold development evidence: 30 earlier model/fold cells are reused and six added leaf-size cells are newly fitted/scored on reconstructed inputs. Leaf sizes 250/500/1000 were compared on the selected 22 features; the locked model uses minimum leaf size 1000. Month/station/dry-wet metrics, eligibility coverage, fixed-bin ECE and exact low-probability point counts accompany the C report.

This is development validation, not a newly untouched final test. No new 2024 scores were computed or used to choose the selected model. The original tree's 2024 Brier 0.01773592/38.59% frequency-baseline improvement remains its own historical result. After development lock the new model refit 4,071,679 sampled 2017–2023 anchors. 6,256 original files retained their hashes.

## Files

- Final_Report.pdf / Final_Report_EN.md: coherent main report covering cleaning, all nine Task 1 figures, the current C model, baselines, ablation, groups and a labelled historical 2024 benchmark.
- Final_Report_Summary_EN.md: concise actual summary, not a copy of the full report.
- Trained_Model.zip: selected conditional-CDF model, predictor contract, exact empirical distributions, frozen configuration and logical-file hashes.
- Source_Code.zip: complete current A/B/C source, formal-data manifests, small fixtures, historical and selected deployed models and compact results. It omits full multi-GB data/features/intermediate anchors/predictions/caches. Fetched B/history documents may be bilingual; C report/source is English.
- DSA5208_Project2_Submission.zip: report, README, source/model and checked file manifest.
- integration_sources.json: explicit Task 1, historical classifier and selected-model evidence directories and selection identity.

All three members are supplied in members.json and the cover: SHAO YUQING A0330603W, YANG AIER A0354958J, ZHU YIXU A0355117H. Canvas upload is not performed; intro.txt deadline is 18 October 2026. Members must review assumptions, code/results and AI disclosure before submission.

## Environment and model loading

Python 3.11, Java 17, PySpark 3.5.7. Install requirements-spark.txt, B_delivery/requirements-B.txt and C_delivery/requirements-C.txt into the active environment. Finalization discovers dependency metadata from the running interpreter, without project-private uv or a hardcoded Windows/macOS venv path. Windows can execute individual Python stages; the shell wrappers target Unix. This model was trained on Mac, not on a Windows machine.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-spark.txt -r B_delivery/requirements-B.txt -r C_delivery/requirements-C.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

Trained_Model.zip extracts to trained_model/. From project root:

```python
import sys
sys.path.insert(0, 'C_delivery/src')
from C_predict import RainfallPredictor
predictor = RainfallPredictor(spark, 'trained_model')
answer = predictor.predict(observations, station_id, prediction_epoch_s, '3.14159')
```

Supply the published observation schema, only past data if desired. All 12 exact endpoints t−3300,...,t must exist; supported station and five-minute grid are required. Threshold can be any finite nonnegative value representable as Decimal(38,18), queried with strict > via an exact empirical CDF. Numeric text/Decimal preserves precision; output also includes threshold_decimal_mm. The empirical survival function is stepwise and nonincreasing. Predictive validation covers nine cuts within 0..5 mm, including four excluded from fitting/selection; extreme-tail reliability is not established. Metadata hashes are checked before loading. No future eligibility/labels gate an inference request.

Fresh-process verification reconstructed 100 anchors from past-only observations and compared 1,000 queries (ten cuts including 3.14159), maximum error zero. Invalid inputs, monotonicity, legacy compatibility and portable finalization passed. This does not prove real-time publication availability; the retrospective event-time assumption remains.

## Reproduce the model

Download the eight rainfall_v1_2017.zip through rainfall_v1_2024.zip archives from https://github.com/yangaier0920-afk/DSA5208_Project2/releases/tag/rainfall_v1_r2 and extract into the project root. The exact raw CSV URLs/hashes are in manifests/raw_manifest.json; full raw processing scripts are in src/. Restore .runtime/frozen_root using submission/tools/prepare_frozen_runtime.py. Regenerate full B features into a fresh output if absent:

```bash
python B_delivery/src/B_build_features.py --root .runtime/frozen_root --output B_delivery/results/features_reproduction
# Then use C_FEATURES="$PWD/B_delivery/results/features_reproduction" in the runner below.
```

The compact source archive does not include full original C fold anchors.

```bash
python C_delivery/src/C_revision_rebuild_anchors.py --root .runtime/frozen_root --features B_delivery/results/features_reproduction --reference-run C_delivery/results/C_v1_20261012 --output C_delivery/results/rebuilt_inputs_reproduction
python C_delivery/src/C_run_leaf_study.py --base-run C_delivery/results/C_revision_20261013 --source-run C_delivery/results/C_v1_20261012 --input-anchors C_delivery/results/rebuilt_inputs_reproduction --root .runtime/frozen_root --features B_delivery/results/features_reproduction --config C_delivery/configs/C_leaf_study.json --output C_delivery/results/C_leaf_reproduction --expected-anchor-signatures C_delivery/results/C_leaf_study_20261014/rebuilt_anchor_comparison.json
python C_delivery/src/C_revision_report.py --run C_delivery/results/C_leaf_reproduction --source-run C_delivery/results/C_v1_20261012
python C_delivery/src/C_finalize.py --run C_delivery/results/C_leaf_reproduction
```

If original anchors are missing, the runner rebuilds 2017–2023 development train/validation plus final TRAIN from formal A/B tables, checks deterministic counts/new joins and records that regenerated Parquet bytes differ from original writes. It does not rebuild/score 2024 TEST. Full feature computation/reconstruction is required; fixture/default-notebook smoke checks do not certify it. The supplementary run actually rebuilt C development/refit inputs from an extracted source directory and used them for the new candidates/refit. Counts and two role-wise row-hash sum signatures match historical inputs; these signatures are diagnostics, not cryptographic equality proofs. The earlier baseline and fixed-leaf feature-ablation results are transparently reused. Original-byte frozen provenance is deliberately separate from active documentation; historical original-language bytes are preserved only for checksum-exact replay.

All new run report/model ZIP/lock/manifest paths derive from the run directory. Integrated reporting takes --task1, --c-run, --c-report, --revision-run and --output explicitly. C_delivery/CURRENT_EVIDENCE.md points to successful current evidence without deleting old failure records. Exact commands and decision protocol are in C_delivery/README.md and docs/C_REVISION_PROTOCOL.md.

AI use is disclosed in AI_USAGE_DECLARATION.md and in a dedicated section of the PDF report. OpenAI Codex assisted implementation, analysis, experiments, checks and drafting. Reported metrics come from saved executed outputs. Members retain responsibility for review and should add any other tools actually used. Remaining improvement leads and supporting roles are in remaining_improvement_ownership.md.

## Rebuild the integrated documents and delivery

```bash
python submission/tools/build_report.py
python submission/tools/build_english_summary.py
python submission/tools/package_submission.py
```

Defaults select the verified Task 1 results and selected conditional-distribution model, while reading the historical classifier only for its benchmark. Canonical deliverables are in submission/. The combined package is DSA5208_Project2_Submission.zip. Original signed metadata and historical experiment evidence are preserved. Report generation uses ReportLab with the available macOS font or a DejaVu Sans fallback.

## Notebook scope and portability

The notebooks are optional viewers; default cells do not certify full recomputation. In B_Historical_Features.ipynb, RECOMPUTE_FEATURES enables full feature writes; in B_Task1_Analysis.ipynb RECOMPUTE_AGGREGATES enables full aggregation; plotting switches are separate. Both B notebooks discover the project root from the current directory or PROJECT_ROOT, and support PROJECT_DATA_ROOT for frozen data. For checksum-verified full runs, use the documented Python scripts with --root .runtime/frozen_root and fresh --output directories. C compact-source anchor reconstruction and its model stages were executed with existing verified A/B data; a full fresh-machine A/B/C run and Windows execution remain untested. See requirements_and_review_acceptance.md for item-by-item evidence and limits.

C_REQUIRED_WORK_COMPLETION.md records the completed C items. the separately supplied Chinese group handoff lists A/B and shared submission actions; optional experiments are excluded.

Portable entry point: `python submission/tools/run_project.py --run-id reproduction_001`. See [portable execution and identity diagnostics](submission/PORTABLE_RUN.md). This entry point creates no ZIP archives and records failed stages explicitly.
