"""Consolidate the team entry point while preserving the previous local release."""
import copy
from pathlib import Path
from data_release import ROOT, read_json, resolve_path, sha256, verify_release, write_json


README = '# A support and reproduction entry points\n\nThis historical package documents completed A data auditing, cleaning, future-truth preparation and representative replay. It does not itself contain B/C model results. Current integrated delivery is in submission/README.md.\n\nUse Python 3.11, Java 17 and requirements-spark.txt; consult docs/environment.md for original Windows native setup. From project root, prepare the frozen runtime using submission/tools/prepare_frozen_runtime.py, then run:\n\n```bash\npython scripts/read_data_release.py --root .runtime/frozen_root --member A --receipt outputs/A_sample_new.json\npython scripts/reproduce_a_sample.py --root .runtime/frozen_root --output outputs/A_replay_new\n```\n\nEvery rerun needs new output names. Paired fixtures have 1,309 rows each; real raw replay uses 1,297 records and 114 anchors including all 86 aligned collisions. Full production reconstruction additionally needs the eight exact annual CSV exports listed in manifests/raw_manifest.json. Historical release/support hashes refer to original bytes; translation is documented separately. Report support includes original environment/runtime, annual ledger, limitations and AI statement; see docs/a_report_section_en.md and outputs/p1_6_support.\n\n\nData v1: observations p0_3_v1, targets p0_4_v1; rainfall_v1_r2 revises documentation only. The small package omits about 4.68 GB of formal tables; obtain them separately at the documented paths. GitHub/download publication was pending at this historical stage.\n'


def main():
    archive = ROOT / 'archive/history_v1'
    if archive.exists():
        raise RuntimeError('Consolidation already exists')
    old = read_json(ROOT / 'manifests/release_manifest.json')
    old_support = read_json(ROOT / 'manifests/a_support_manifest.json')
    verify_release(ROOT, 'full')
    docs = {p.name: p.read_text(encoding='utf-8') for p in (ROOT / 'docs').glob('*.md')}
    brief = (ROOT / 'project_requirement.md').read_text(encoding='utf-8')
    moves = [(p, archive / 'docs' / p.name) for p in (ROOT / 'docs').glob('*.md')]
    moves += [(ROOT / n, archive / n) for n in ['README.md', 'README_A_P1_6.md', 'project_requirement.md', 'TEAM_WORK_PLAN.md', 'A_DATA_FOUNDATION_PLAN.md']]
    moves += [(ROOT / 'data/samples/p0_1_v1', archive / 'data/samples/p0_1_v1')]
    moves += [(ROOT / f'manifests/{n}', archive / f'manifests/{n}') for n in ['release_manifest.json', 'a_support_manifest.json']]
    for source, destination in moves:
        if not source.resolve().is_relative_to(ROOT) or not destination.resolve().is_relative_to(ROOT) or not source.exists() or destination.exists():
            raise RuntimeError(f'Unsafe or unavailable archive move: {source}')
    mapping = {s.relative_to(ROOT).as_posix(): d.relative_to(ROOT).as_posix() for s, d in moves}
    def remap(relative):
        for source, target in mapping.items():
            if relative == source or relative.startswith(source + '/'):
                return target + relative[len(source):]
        return relative
    for source, destination in moves:
        destination.parent.mkdir(parents=True, exist_ok=True)
        source.rename(destination)
    # Preserve old validation/fixture identity before changing the documentation release.
    fixture = ROOT / 'data/samples/p1_6_v1/fixture_manifest.json'
    backup = archive / 'data/samples/p1_6_v1/fixture_manifest.json'
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_bytes(fixture.read_bytes())
    (ROOT / 'README.md').write_text(README, encoding='utf-8')
    ref = "# Project reference material\n\nDaily instructions: README.md. Historical data v1/contract and original byte identity are preserved.\n\n# Project responsibilities and delivery plan\n\nUse the 2017–2024 Singapore five-minute rainfall datasets with Apache Spark. Task 1 explores changes within and across years with visual evidence. Task 2 predicts P(next-30-minute accumulated rainfall > input threshold) for a station and prediction time, using Spark MLlib.\n\n| Member role | Responsibility | Deliverable |\n|---|---|---|\n| A | Audit raw annual CSV, clean and standardize observations, preserve provenance, construct exact future-window truth and time splits | Versioned Parquet, coverage tables, data contract, environment and replay evidence |\n| B | Coverage-aware seasonal, hourly, station and interannual analysis; past-only features | Spark analysis source, figures, summaries, feature contract and Parquet |\n| C | Threshold expansion, temporal validation, frequency/LR/tree candidates, probability evaluation and inference | Saved model, evaluation tables, input contract and reload checks |\n\nThe members jointly review assumptions, reproduce interfaces, integrate the report and submit complete code with README, the trained model and a PDF listing all names and student IDs. The deadline in the course instructions is 18 October 2026; the planned development period is 8–18 October. At most three members are allowed.\n\nStart B/C interface work with small fixtures while A performs full processing. Freeze common time, missingness, key and partition rules before final experiments. Compare coverage-qualified stations, never nationwide sums over changing station sets. Compare a training-frequency baseline, logistic regression and a shallow tree; choose using development data and reserve 2024 for the final test. Record AI use, limitations and measured computing resources. Check data identity, unique joins, no future leakage, model persistence, past-only inference and complete submission materials.\n\n# Released data contract\n\nData version v1; observations p0_3_v1; targets p0_4_v1. Original releases rainfall_v1/r2/r3 changed delivery documentation without changing Parquet rules. The immutable original-byte snapshot and checksums are retained separately after English translation.\n\nFormal observations and targets each have 48,538,627 rows. Keys are station_id+slot_end_epoch_s and station_id+prediction_epoch_s respectively. Read using scripts/data_release.py and match both station and epoch; do not join only station/date. Use Asia/Singapore, Unix seconds and rainfall mm. Exact rainfall is Decimal(38,18).\n\nOnly 2017 epoch remainder 299 is shifted +1 second. Equal-rain collisions prefer the original standard-grid record, retaining two-sided lineage; unequal-rain groups are quarantined. Valid finite nonnegative rainfall, zeros and extremes are retained. Missingness remains unknown. No future metadata backfill. Metadata first/last appearance is not a continuous validity interval.\n\nThe project assumes an interval endpoint t with R(t) available at t. Task 1 calendar uses t−300 seconds. Future truth is the six exact endpoints t+300,...,t+1800; full history is 12 endpoints t−3300,...,t. Load boundary context before filtering years. Unknown future truth stays null; strict exceedance uses >, not >=. Coverage comparison uses the same qualified stations for each supported month, full calendar denominators, main 90% and sensitivity 95%; it does not globally filter Task 2.\n\nE2 development: 2017–2020/2021, 2017–2021/2022, 2017–2022/2023. Lock selection then refit 2017–2023/test 2024. Filter actual role boundaries and joint window eligibility for supervision; do not remove every cross-year anchor. Fit learned encoding/scaling only on training, sample anchors before threshold expansion. Future values, eligibility and role are not features or inference gates.\n\nRecord data_version, release_id, config_hash (SHA-256 of exact original project.json bytes) and experiment_config_hash in result metadata. Do not silently replace historical identities. New data/rules require a new data version; new delivery documentation requires a documented revision. Original frozen bytes must pass their original checksums. New translated documents have their own translation inventory. Paired 1,309-row fixtures and 1,297-record/114-anchor replay are interface tests, not representative training data.\n\nSee p0_3_data_contract.md, p0_4_data_contract.md, field_dictionary.md and E2_ROLLING_VALIDATION.md for details. Full source, trained model and integrated report are now in submission; historical A-stage status files retain their original scope.\n\n# E2 expanding-window validation protocol\n\nHistorical protocol, confirmed before formal modelling. A prepares truth and role fields; B computes past-only features; C implements sampling, threshold expansion and Spark MLlib evaluation. The protocol itself does not claim that training had already been executed at this historical stage.\n\n| Fold | Training years | Validation years |\n|---|---|---|\n| fold1 | 2017–2020 | 2021 |\n| fold2 | 2017–2021 | 2022 |\n| fold3 | 2017–2022 | 2023 |\n| Final, after selection | 2017–2023 | 2024 TEST |\n\nUse explicit temporal filters, not ordinary random-fold CrossValidator. Require full past 60 minutes (12 exact slots) and full future 30 minutes (six exact slots). Exclude anchors crossing the actual partition boundary, including a label endpoint equal to the next block start. Internal calendar-year boundaries inside a training block remain valid when context is complete. Keep EXCLUDED_BOUNDARY and UNUSED out of that fold.\n\nPilot on 1% deterministic hash-sampled anchors; formal training on 10%. Score validation and test at full eligible scope. Sample anchors before expanding all thresholds, keep all threshold versions of an anchor together, and do not sample by outcome. Candidate thresholds are 0,0.5,1,2,5 mm, supported and frozen using only the earliest 2017–2020 training block. Both classes must have adequate training support. Numeric threshold is an input to the shared model; inference is restricted to supported thresholds and training-known stations.\n\nCandidates are a threshold-specific training-frequency baseline, logistic regression and a shallow decision tree. Fit encoding, scaling and any learned feature transformations only on each fold's training data. No future truth, completeness, eligibility or role fields may enter the vector. Compare Brier with equal threshold weight within year, then equal weight across the three validation years. Report log loss, PR-AUC, ROC-AUC and reliability as additional diagnostics. Predefine candidate configurations and a simplicity preference for effectively tied scores.\n\nLock selection before the final 2017–2023 refit. Use 2024 only for the final evaluation; never choose the model, thresholds, features or calibration based on its scores. An optional E1 comparison (train 2017–2022, validate 2023) is diagnostic and must not use 2024 to choose a protocol. Save per-threshold/year metrics, support counts, computational settings, model and reload/inference evidence. The completed implementation is documented in C_delivery and the integrated report.\n\n\n## Complete field dictionary\n\n<details><summary>189 fields</summary>\n\n"
    dictionary = docs['field_dictionary.md'].split('\n', 2)[2]
    dictionary = dictionary.replace('See data_contract', 'See this document data/target rules')
    ref += dictionary + '\n</details>\n\n## A cleaning report material\n\n'
    report = docs['a_cleaning_report_zh.md'].split('## Data source and full counts', 1)[1]
    report = ('### Data source and full counts' + report).replace('\n## ', '\n### ')
    report = report.replace('docs/environment.md', 'archive/history_v1/docs/environment.md').replace('English draft: a_report_section_en.md', 'English draft in the next section')
    ref += report + '\n\n## A English report chapter\n\n'
    english = docs['a_report_section_en.md'].split('## Data and cleaning', 1)[1]
    ref += ('### Data and cleaning' + english).replace('\n## ', '\n### ')
    ref += '\n\n## Reproduction and evidence\n\nRead_data_release checks interfaces; reproduce_a_sample replays 1,297 real records and 114 anchors; record_result_metadata records downstream provenance. Full raw stages require exact annual CSV and fresh isolated evidence paths. Historical A-only checks do not establish B/C or another OS. Data/config changes require new identities; never overwrite original results. Current full group results are in submission.\n\n## Original course instructions\n\n<details><summary>Course text</summary>\n\n'
    ref += brief + '\n</details>\n'
    (ROOT / 'docs/reference.md').write_text(ref, encoding='utf-8')
    current = copy.deepcopy(old)
    current.update({'release_id': 'rainfall_v1_r2', 'previous_release_manifest': 'archive/history_v1/manifests/release_manifest.json',
                    'revision_scope': 'Documentation consolidation only; data/configuration/table paths unchanged',
                    'interface_contract': 'README.md', 'field_dictionary': 'docs/reference.md', 'reference_document': 'docs/reference.md'})
    current['files'] = [{**i, 'path': remap(i['path'])} for i in old['files']]
    for path in ['README.md', 'docs/reference.md', 'scripts/consolidate_handoff.py']:
        p = ROOT / path
        current['files'].append({'path': path, 'bytes': p.stat().st_size, 'sha256': sha256(p), 'kind': 'metadata'})
    write_json(ROOT / 'manifests/release_manifest.json', current)
    fragment = read_json(fixture)
    fragment.update({k: current[k] for k in ['release_id', 'data_version', 'config_hash']})
    fragment['release_manifest_sha256'] = sha256(ROOT / 'manifests/release_manifest.json')
    write_json(fixture, fragment)
    support = copy.deepcopy(old_support)
    support.update({'support_version': 'a_support_v1_r2', 'release_id': current['release_id'],
                    'release_manifest_sha256': sha256(ROOT / 'manifests/release_manifest.json'), 'entrypoint': 'README.md',
                    'previous_support_manifest': 'archive/history_v1/manifests/a_support_manifest.json'})
    support['files'] = [{**i, 'path': remap(i['path'])} for i in old_support['files']]
    for i in support['files']:
        if i['path'] == 'data/samples/p1_6_v1/fixture_manifest.json':
            i.update({'bytes': fixture.stat().st_size, 'sha256': sha256(fixture)})
    for p in ['README.md', 'docs/reference.md']:
        path = ROOT / p
        support['files'].append({'path': p, 'bytes': path.stat().st_size, 'sha256': sha256(path)})
    write_json(ROOT / 'manifests/a_support_manifest.json', support)
    write_json(ROOT / 'outputs/team_handoff/consolidation_record.json', {
        'status': 'PREPARED_PENDING_VALIDATION', 'release_id': current['release_id'], 'data_version': current['data_version'],
        'config_hash': current['config_hash'], 'archive_mapping': mapping,
        'preserved_formal_parquet': [{k: i[k] for k in ['path','bytes','sha256']} for i in old['files'] if i['kind'] == 'full_data']})
    print('Prepared one README and one reference document; old originals archived; data/config unchanged')


if __name__ == '__main__':
    main()
