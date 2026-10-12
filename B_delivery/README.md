# B: rainfall exploration and historical features

Completed Task 1 and 26 past-only numerical features for C. Formal data are A's observations p0_3_v1; paired targets share the station/epoch key. Read the project README for pinned Python 3.11, Java 17, Spark 3.5.7 and formal-data downloads. Prepare the immutable frozen runtime before hash-validated reads.

Run individual source entry points with --help or the full-project runner. B_task1_analysis.py aggregates coverage-aware monthly/annual/hourly/station summaries; B_plot_results.py renders seven figures from small CSVs; B_build_features.py writes exact-time past features; B_validate_feature_api.py checks production/inference consistency; B_join_audit.py verifies key matching. Use fresh output roots on reruns. The complete project runner is submission/tools/run_full_project.sh.

Original results are results/task1_v2 and features_v2; current Mac reruns are task1_rerun_20261012, features_rerun_20261012 and join_audit_rerun_20261012. Reports are English in report_zh/B_report_zh.md and report_rerun_20261012/B_report_zh.md; legacy Latin filenames are retained for compatibility. Notebook entry points are notebooks/B_Task1_Analysis.ipynb and B_Historical_Features.ipynb, with expensive recomputation disabled by default.

See docs/TIME_CONTRACT.md, FEATURE_DEFINITIONS.md and feature_definitions.csv for exact endpoints, missingness, censoring and all 26 definitions. Missing values are not zero-filled; 48,538,627 feature anchors remain and 47,347,833 have complete history. Task 1 main coverage is 90%, sensitivity 95%; fixed eight-year same-month panels support May/September/October/December only. Do not interpret empty panels or incomplete annual sums as no rain or national trends.

C's completed modelling results are in C_delivery; integrated conclusions and AI disclosure are in submission/Final_Report_EN.md. B's historical reports did not claim model scores before C execution. Numbers are preserved through English translation.
