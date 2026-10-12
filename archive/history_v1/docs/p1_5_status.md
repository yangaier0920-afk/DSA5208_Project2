# P1-5 frozen handoff and sample acceptance

Historical A-stage record. Data v1 was frozen as rainfall_v1 before later documentation releases; Git publication had not yet occurred at that stage. The paired observation/target fixture contains 1,309 rows per table across eight years. Readers verify release hashes, configuration identity and schema before use. B/C acceptance and C modelling were then pending; they are now separately documented in the integrated report.

B uses observations/coverage for Task 1 and past-only features. C filters target role/joint eligibility and joins features on station_id+prediction_epoch_s before threshold expansion. Missing future truth cannot become a negative example. Samples are not formal training substitutes. Record exact configuration SHA and experiment parameters in result metadata. Progress metadata is distinct from immutable delivery identity.

Historical helpers: scripts/prepare_release_sample.py, prepare_data_release.py, read_data_release.py --member A and verify_handoff.py. Repeat tests in a fresh receipt/output directory. Same-machine relocated-directory acceptance verifies path portability, not another operating system. See the immutable release snapshot for original identities and current README for English reproduction.
