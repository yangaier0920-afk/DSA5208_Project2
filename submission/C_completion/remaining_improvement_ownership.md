# Remaining improvement ownership

Role boundaries follow docs/reference.md: A owns data/provenance/labels and reproduction support; B owns Task 1 and historical features; C owns modelling/evaluation/inference. Final review and submission are shared. The repository supplies three names but does not explicitly map names to A/B/C, so this table identifies responsibilities by role rather than assigning unverified personal identities. Lead/support below are proposed allocations based on the documented boundaries, not evidence that a member has accepted the work.

| Remaining item | Lead role | Supporting role | Completion criterion |
|---|---|---|---|
| Fresh-environment full reproduction | A coordinates | B regenerates analysis/features; C reconstructs anchors, trains/evaluates/finalizes | From extracted source and downloaded formal data, complete every stage and record reproducible commands/receipts |
| Notebook paths, frozen root and view/recompute controls | B | A validates data/runtime interfaces | Remove machine-specific PROJECT_ROOT; use explicit verified data root; new outputs; distinguish viewing from full computation |
| Independent leaf-size comparison | C: COMPLETE | B feature semantics retained | Leaf250/500/1000 comparison, locked selection, refit/reload and packaging completed; see C_REQUIRED_WORK_COMPLETION.md |
| Windows runtime acceptance, if Windows support is required | A coordinates environment | B/C verify their stages, with C responsible for finalization/model API | Execute documented setup and relevant pipeline/model/package commands; record real receipts rather than inferred compatibility |
| Paired time-block uncertainty estimates (optional) | C | B assists temporal/weather grouping; A supplies stable keys | Resample day/week blocks retaining all stations and pair models on the same blocks; no independent-row bootstrap |
| Wider threshold and empirical-tail validation (optional) | C | B assists covariate/feature diagnostics | Use development or independent new data, record event/support counts and calibration, delimit validated range |
| Independent later test and real-time availability study (later work) | C leads evaluation; A leads availability contract | B adapts historical features | Obtain independent later observations and publication timing; redefine available inputs if needed; no new selection using known 2024 |
| AI declaration, final content review and Canvas upload | All three members jointly | Submission uploader chosen by group | Each member verifies their actual AI use and content; upload complete source/README, model and named PDF by 18 October 2026 |

The stronger baselines, current controlled feature-group ablation and subgroup evaluation are already completed C modelling tasks, with B supplying feature definitions. They are not outstanding new B-only work. No additional experiment above is required merely to relabel an already completed result; optional scientific improvements should remain clearly separate from mandatory submission work.

C non-optional model/reconstruction stages are completed. Remaining A/B and shared actions are consolidated in the separately supplied Chinese group handoff. Optional research rows above are not part of the requested completion work.
