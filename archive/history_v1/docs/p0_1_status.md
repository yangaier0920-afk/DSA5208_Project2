# P0-1 environment and shared-sample status

Historical status, 8 October 2026: A passed environment setup and the shared sample smoke test. B/C acceptance was deferred by the user; this is not evidence that their machines had passed. Subsequent completed B/C work is documented in submission.

Pinned Python 3.11, Java 17, PySpark 3.5.7 and Py4J 0.10.9.7. The shared fixture has 16,000 records, 2,000 per year for 2017–2024, 473 station/year groups, approximately 134 KiB. It validates schema, Spark reads and interfaces only, not representativeness. Physical line counts in the raw manifest are preliminary input inventory, not cleaned counts. Local input hashes identify acquired bytes; remote server hashes were not captured.

See environment.md and shared_sample_readme.md. Preserve original files and machine-specific receipts, use new output paths on reruns, and report untested platforms accurately.
