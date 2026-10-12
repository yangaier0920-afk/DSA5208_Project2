# A support and reproduction entry points

This historical package documents completed A data auditing, cleaning, future-truth preparation and representative replay. It does not itself contain B/C model results. Current integrated delivery is in submission/README.md.

Use Python 3.11, Java 17 and requirements-spark.txt; consult docs/environment.md for original Windows native setup. From project root, prepare the frozen runtime using submission/tools/prepare_frozen_runtime.py, then run:

```bash
python scripts/read_data_release.py --root .runtime/frozen_root --member A --receipt outputs/A_sample_new.json
python scripts/reproduce_a_sample.py --root .runtime/frozen_root --output outputs/A_replay_new
```

Every rerun needs new output names. Paired fixtures have 1,309 rows each; real raw replay uses 1,297 records and 114 anchors including all 86 aligned collisions. Full production reconstruction additionally needs the eight exact annual CSV exports listed in manifests/raw_manifest.json. Historical release/support hashes refer to original bytes; translation is documented separately. Report support includes original environment/runtime, annual ledger, limitations and AI statement; see docs/a_report_section_en.md and outputs/p1_6_support.
