# DSA5208 Project 2 - submission and reproducibility

This project integrates the completed A/B/C work against `intro.txt`. Data are 2017-2024 Singapore rainfall, cleaned with Spark; Task 1 provides coverage-aware visual exploration; Task 2 uses Spark MLlib for 30-minute exceedance probabilities. No new model tuning uses 2024 in this integration step.

## Submission files

- `Final_Report.pdf`: integrated English report with 10 figures and complete development-metric appendices.
- `Final_Report_EN.md`: editable English source.
- `Final_Report_Summary_EN.md`: editable English summary.
- `Source_Code.zip`: complete A/B/C source, fixed configurations, README, sample fixtures, report sources and compact evidence.
- `Trained_Model.zip`: final fitted Spark PipelineModel, station/history/threshold contract, model-file hashes and experiment configuration.
- `submission_manifest.json`: hashes and requirement status for the prepared materials.

All three members are listed in the PDF cover and members.json: SHAO YUQING (A0330603W), YANG AIER (A0354958J), ZHU YIXU (A0355117H), as supplied by the user. Report, source and trained-model materials are ready for user submission. The package has not been uploaded to Canvas; the deadline stated in intro.txt is 18 October 2026.

The source archive has also been extracted into a fresh directory: frozen runtime preparation, 1,309-row paired sample read, 1,297-record/114-anchor A replay and deployed-model past-only predictions all passed. Receipts are in verification/.

## Environment

Tested: macOS 14.1, Apple M3 Max, 36 GB memory, Python 3.11.17, Java 17.0.20.1, PySpark 3.5.7. B uses local[2]/4g/128 shuffle partitions; C uses local[2]/6g/128. A's historical full raw processing was on Windows/i7-12700H; the report separates those recorded results from current Mac replay.

From the extracted `DSA5208_Project2` project root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-spark.txt -r B_delivery/requirements-B.txt -r C_delivery/requirements-C.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1
```

Windows uses Python 3.11 and Java 17 with `.venv\Scripts\python.exe`; native setup helpers are retained in `scripts/setup_java17.py` and `scripts/setup_hadoop_windows.py`. The shell workflow is macOS/Linux; Windows can execute its Python commands individually. Model formats, Python minor, Spark version, Java major and timezone must agree. Existing `.runtime` and virtual environments are machine-local and not shipped.

## Inspect/verify original results without retraining

Extract `Source_Code.zip`. The ZIP contains compact Task 1 tables, figures, C metric/coverage/audit JSON and CSV, and the deployed model. It omits the multi-GB raw exports, full published data, full B feature Parquet, fold anchor tables, per-row predictions and Spark caches. These are separately downloadable or regenerated, not needed to read the report. Original run metadata describes the original disk outputs, including intermediates omitted from this source ZIP; the ZIP's own payload manifest defines what is actually shipped.

```bash
python submission/tools/prepare_frozen_runtime.py
python scripts/read_data_release.py --root .runtime/frozen_root --member C \
  --receipt outputs/verify_sample_001.json
python scripts/reproduce_a_sample.py --root .runtime/frozen_root \
  --output outputs/verify_A_001
python C_delivery/src/C_test_contracts.py
```

Use new receipts/output directories on repeat runs. The shipped samples are interface/reproduction fixtures, not formal training substitutes. The portable helper restores immutable original release bytes and checksum manifest from the archived metadata ZIP into an isolated hardlink/copy tree. It preserves current source and never rewrites the release manifest or dataset rules. The current resource-adjusted full reader is used with that data tree when full data are present.

## Load and predict with the trained model

`Trained_Model.zip` extracts to `trained_model/`. The source ZIP also contains this model under `C_delivery/results/C_v1_20261012/trained_model/`. Use:

```python
import sys
sys.path.insert(0, 'C_delivery/src')
from C_predict import RainfallPredictor
predictor = RainfallPredictor(spark, 'trained_model')
result = predictor.predict(observations, station_id, prediction_epoch_s, threshold_mm)
```

`observations` uses A's published schema. Supply only the station's observed past if desired; all 12 exact endpoints t-3300,...,t must be valid. Time must lie on a five-minute grid. Threshold support is frozen to 0,0.5,1,2,5 mm, and the station must occur in final sampled training. Unsupported inputs raise ValueError. The inference policy does not require future labels or future eligibility. Timestamp interval/availability uses the documented retrospective assumption; publication latency has not been proven suitable for live deployment.

## Download formal data and reproduce Task 1 and Task 2

The eight official project Parquet release archives are at:
https://github.com/yangaier0920-afk/DSA5208_Project2/releases/tag/rainfall_v1_r2

```bash
mkdir -p downloads
for year in {2017..2024}; do
  curl -fL --retry 3 \
    "https://github.com/yangaier0920-afk/DSA5208_Project2/releases/download/rainfall_v1_r2/rainfall_v1_${year}.zip" \
    -o "downloads/rainfall_v1_${year}.zip"
  unzip -n "downloads/rainfall_v1_${year}.zip" -d .
done
# New outputs; do not overwrite original experiment records.
PROJECT_RUN_ID=reproduction_001 bash submission/tools/run_full_project.sh
```

Downloads total roughly 2.70 GB, formal Parquet about 4.68 GB, plus generated features/anchors/predictions and temporary shuffle files. Maintain adequate free disk space. The full workflow prepares the frozen tree, verifies all data, replays A fragments, recomputes Task 1/figures/features, audits joins, runs C 1% pilot, three formal 10% rolling folds, locks selection, refits and evaluates 2024, packages/reloads and regenerates C evidence. Reproduction should preserve the prespecified protocol; do not retune on the known 2024 results.

To reproduce A's entire raw processing instead of using published Parquet, obtain the eight exact annual CSV files listed in `manifests/raw_manifest.json` (dataset URLs and required SHA-256 included) and put them at its `Raw_Dataset/...` paths. Full production source is in `src/01_audit_raw.py`, `02_clean_standardize.py`, `03_build_targets.py` and their verification scripts. Use fresh workspace-owned output/evidence paths; original run receipts are retained and will deliberately block silent overwrite. A full-raw rerun is optional for the B/C released-data workflow, not replaced by the small fixture test. Original full-raw results are documented in the supplied evidence and report.

## Regenerate the integrated report and packages

Edit `submission/members.json` with actual values, for example an object with `members` containing objects with `name` and `student_id` fields; no sample identities are supplied. From project root:

```bash
python submission/tools/build_report.py
python submission/tools/build_english_summary.py
python submission/tools/package_submission.py
```

PDF generation embeds macOS Heiti SC by default. Other systems set `C_FONT_PATH` to a Unicode TrueType font (TTF or the first face of a TTC). Report-generation dependencies are reportlab, matplotlib and their requirements above. Current integrated reports are based on the preserved `task1_rerun_20261012`/`C_v1_20261012` evidence, not an arbitrary later run; update report source paths explicitly to integrate a new experiment.

## Protocol and headline outcome

Strict Decimal future sum > threshold; equality negative, missing future excluded. B's 26 past-only inputs plus numerical threshold and training-only station one-hot coding. Fixed 10% hash sample, seed=20261012, before threshold expansion. Development: 2017-2020→2021, 2017-2021→2022, 2017-2022→2023. Equal-threshold/equal-year mean Brier chooses the depth-5 tree over frequency and LR. Final 2017-2023 refit, 2024 once: Brier 0.017736 versus 0.028883 baseline, improvement 38.59%. Alternative diagnostics and all limitations remain reported.

AI use: OpenAI Codex assisted planning, code, execution, quality/reproduction checks, visualization and report drafting. Actual output metrics and synthetic tests are distinguished. Group members must review the content and supplement disclosures for any other tools actually used.

## English documentation revision

The pinned submission documents and reproduction materials are English. The latest B editable report and Task 1 notebook are Chinese; the separate English B reference is in B_delivery/report_en. Other legacy `_zh` filenames may contain English, as documented by their package README. Original release bytes are stored only in the immutable provenance ZIP described in reproduction/README.md, restored into hidden .runtime for checksum-exact replay. Translated receipt machine paths are display labels, not executable paths. Data/model/metrics are unchanged. See submission/intro_requirements_check.md for the course checklist and supported-threshold limitation.

## B revision after team integration (2026-10-13)

The latest B Task 1 notebook and Chinese report now contain nine figures, reordered as coverage, within-year patterns, interannual comparisons and spatial differences. See `B_delivery/README.md` and `B_delivery/report_zh/B_report_zh.md`. The teammate's English B reference is preserved at `B_delivery/report_en/B_report_en.md`. C results and the pinned 2026-10-12 reproduction evidence are retained. The integrated PDF and submission ZIPs have not been rebuilt by this merge and do not yet include the two additional B figures.
