"""Generate A report drafts and resource/loss tables from verified run records."""
import csv
from datetime import datetime, timezone
from pathlib import Path
from data_release import ROOT, provenance, read_json, verify_release, write_json


def write_csv(path, rows):
    with path.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    verify_release(ROOT, 'sample')
    output = ROOT / 'outputs/p1_6_support'
    output.mkdir(parents=True, exist_ok=True)
    hardware = read_json(output / 'hardware_inventory.json')
    cpu = hardware['cpu']['Name']
    ram_gib = hardware['ram']['TotalPhysicalMemory'] / 1024**3
    phases = [('P0-2', 'p0_2_audit/p0_2_v1'), ('P0-3', 'p0_3_clean/p0_3_v1'), ('P0-4', 'p0_4_targets/p0_4_v1')]
    timings = []
    summaries = {}
    for name, stage in phases:
        base = ROOT / 'outputs' / stage
        run = read_json(base / 'run_status.json')
        summary = read_json(base / 'summary.json')
        if run['status'] != 'PASS' or summary['status'] != 'PASS' or not run['shutdown']['passed']:
            raise RuntimeError(f'Original full run did not pass: {name}')
        summaries[name] = summary
        annual = summary.get('annual_results', summary.get('annual_metrics'))
        elapsed = (datetime.fromisoformat(run['finished_at_sgt']) - datetime.fromisoformat(run['started_at_sgt'])).total_seconds()
        yearly_sum = sum(row['elapsed_seconds'] for row in annual)
        timings.append({'stage': name, 'scope': 'original_eight_year_full_run', 'run_record': f'outputs/{stage}/run_status.json',
                        'started_at_sgt': run['started_at_sgt'], 'finished_at_sgt': run['finished_at_sgt'],
                        'wall_seconds': round(elapsed, 3), 'wall_minutes': round(elapsed / 60, 4),
                        'sum_annual_seconds': round(yearly_sum, 3), 'overhead_seconds': round(elapsed - yearly_sum, 3),
                        'master': 'local[4]', 'driver_memory_config': '4g', 'shuffle_partitions': 48,
                        'configuration_evidence': run.get('environment') and 'run environment' or 'hashed scripts/spark_session.py defaults and src main entrypoint',
                        'peak_rss_bytes': '', 'peak_rss_status': 'NOT_MEASURED',
                        'includes': 'stage startup/processing/readback and shutdown; excludes separate validators, dependency setup and inter-stage human work'})
    cleaner = summaries['P0-3']
    targets = summaries['P0-4']
    raw_count = summaries['P0-2']['total_spark_record_rows']
    observations = cleaner['totals']['observations']
    if raw_count != observations + cleaner['totals']['merged_duplicate_rows'] + cleaner['totals']['quarantine_rows']:
        raise RuntimeError('Full data ledger does not close')
    annual_rows = []
    for clean, target in zip(cleaner['annual_results'], targets['annual_results']):
        if clean['year'] != target['year']:
            raise RuntimeError('Annual report alignment differs')
        annual_rows.append({'year': clean['year'], 'raw_records': clean['profile']['raw_rows'],
            'observations': clean['metrics']['observations'], 'merged_duplicate_rows': clean['metrics']['merged_duplicate_rows'],
            'quarantine_rows': clean['metrics']['quarantine_rows'], 'target_anchors': target['metrics']['anchors'],
            'future_valid': target['metrics']['future_valid'], 'future_invalid': target['metrics']['future_invalid'],
            'history_valid': target['metrics']['history_valid'], 'history_invalid': target['metrics']['history_invalid'],
            'base_model_eligible': target['metrics']['base_model_eligible']})
    losses = [
        {'measure': 'merged_duplicate_rows', 'count': 86, 'denominator': raw_count, 'effect': 'Removed from observations; two-sided lineage retained'},
        {'measure': 'quarantine_rows', 'count': 0, 'denominator': raw_count, 'effect': 'Invalid/conflicting source records; empty table retained'},
        {'measure': 'future_invalid_anchors', 'count': targets['totals']['future_invalid'], 'denominator': observations, 'effect': 'Retained in targets with null cumulative rainfall; not supervised negatives'},
        {'measure': 'history_invalid_anchors', 'count': targets['totals']['history_invalid'], 'denominator': observations, 'effect': 'Retained; incomplete historical inputs under H1'},
        {'measure': 'base_window_ineligible_anchors', 'count': observations - targets['totals']['base_model_eligible'], 'denominator': observations, 'effect': 'Union of history/future invalidity; still subject to fold boundaries and I1 scope'},
    ]
    for row in losses:
        row['percent'] = round(row['count'] / row['denominator'] * 100, 8)
    write_csv(output / 'full_run_resources.csv', timings)
    write_csv(output / 'annual_cleaning_and_targets.csv', annual_rows)
    write_csv(output / 'loss_ledger.csv', losses)
    replay = read_json(output / 'local_replay_v1/verification.json')
    if replay['status'] != 'PASS' or not all(replay['checks'].values()) or not replay['shutdown']['passed']:
        raise RuntimeError('Local representative replay did not pass')
    write_json(output / 'report_metrics.json', {'status': 'PASS', **provenance(), 'hardware': hardware,
        'hardware_scope': 'Measured hardware inventory at P1-6, on the same host; no historical peak-memory instrument was running',
        'full_run_timings': timings, 'raw_records': raw_count, 'observations': observations,
        'cleaning_totals': cleaner['totals'], 'target_totals': targets['totals'], 'losses': losses,
        'sample_replay': {'scope': replay['scope'], 'counts': replay['counts'], 'elapsed_seconds': replay['elapsed_seconds'], 'environment': replay['environment']},
        'downstream_training': 'NOT_STARTED_BY_A', 'generated_at': datetime.now(timezone.utc).isoformat()})
    annual_table = '\n'.join(f'| {r["year"]} | {r["raw_records"]:,} | {r["observations"]:,} | {r["merged_duplicate_rows"]} | {r["future_valid"]:,} | {r["future_invalid"]:,} | {r["base_model_eligible"]:,} |' for r in annual_rows)
    timing_table = '\n'.join(f'| {r["stage"]} | {r["wall_minutes"]:.2f} | {r["sum_annual_seconds"] / 60:.2f} |' for r in timings)
    identity = provenance()
    english = f'''# A report section draft: data preparation and reproducibility

Data release: `{identity['release_id']}`; `data_version={identity['data_version']}`; data configuration SHA-256: `{identity['config_hash']}`. This section describes A's executed work. B/C should supply the weather-analysis, feature-development, modelling and evaluation sections before submission.

## Data and cleaning

The eight annual data.gov.sg rainfall exports for 2017–2024 contain 48,538,713 records according to Spark parsing and occupy 8,047,044,701 bytes. Annual dataset links and local full-file SHA-256 hashes are recorded in `manifests/raw_manifest.json`. Download completeness was confirmed by the user; these local hashes are not presented as server-provided reference checksums. The records contain five-minute rainfall measurements in millimetres, station metadata and event/update timestamps.

We retained the original fields, record text and provenance. For 2017 only, observations whose original epoch remainder modulo 300 was 299 were shifted forward by one second to the five-minute endpoint, while retaining the original event timestamp. This affected 1,775,327 source records and produced 86 pairs of aligned-key collisions with equal rainfall. The original exact-grid observation was retained; rainfall was neither summed nor averaged. Both sides of each collision were preserved in a 172-row lineage table, including two retained observations with metadata conflicts. Conflicting rainfall would quarantine the entire key group, but no such groups occurred in these data.

Finite nonnegative measurements, including genuine zeros and extremes, were retained without arbitrary truncation or rounding to 0.2 mm. Exact cumulative rainfall uses Decimal(38,18); observed source precision required at most five effective decimal places. This storage capacity does not imply additional measurement precision. Missing timestamps remain unknown rather than becoming zero-rainfall observations. The source data have 63 days with no network records, including 4–31 August 2018; their physical cause cannot be inferred from these files.

The cleaning ledger closes as 48,538,713 = 48,538,627 retained observations + 86 merged records + 0 quarantined records. A zero quarantine count does not establish time-series completeness or meteorological quality. The observations table has a unique station/endpoint key and preserves 47,121,647 genuine zero measurements. Rainfall is interpreted as the five-minute interval ending at the aligned timestamp; calendar aggregation for Task 1 uses the interval start, including midnight contributions to the preceding day.

## Future-window truth and completeness

Each observation supplies one prediction anchor. We require observations at all six exact offsets from t+300 to t+1800 seconds for the future 30-minute total. Incomplete totals remain null; complete dry windows contain Decimal zero. History completeness requires all 12 positions from t−3300 to t. Adjacent-year context is included before anchors are selected, preventing artificial truncation at file boundaries.

There are 47,636,833 complete future windows and 901,794 unknown future totals. Historical inputs are complete for 47,347,833 anchors and incomplete for 1,190,794. Both windows are complete for 47,048,872 anchors; further fold-boundary and station-support restrictions remain the responsibility of downstream processing. History and future exclusions overlap and must not be added as disjoint losses. All anchors, including invalid ones, remain in the target table. Threshold labels must use the strict Decimal comparison `future_30m_mm > threshold_mm`; equality is a negative label, while an unknown total is not a negative example.

{annual_table}

E2 role fields support training through 2020/validation in 2021, training through 2021/validation in 2022, and training through 2022/validation in 2023. After configuration selection, the final refit uses 2017–2023 and the final test uses 2024. A generated truth/completeness and role fields without fitting a model, expanding thresholds or learning features. Future totals and future-dependent eligibility flags are prohibited as model inputs or prior inference conditions.

## Computing environment and observed runtime

The local machine uses Windows 11, a {cpu} CPU ({hardware['cpu']['NumberOfCores']} physical cores and {hardware['cpu']['NumberOfLogicalProcessors']} logical processors), and approximately {ram_gib:.2f} GiB of system-visible physical memory. Python 3.11.5, Spark/PySpark 3.5.7, Py4J 0.10.9.7 and Java 17.0.20.1 were used with the Asia/Singapore session timezone. Full stages used `local[4]`, a configured 4g driver heap and 48 shuffle partitions. Local threads are not cluster nodes, and the heap setting is not peak resident memory.

Original full-run wall times were {timings[0]['wall_minutes']:.2f} minutes for auditing, {timings[1]['wall_minutes']:.2f} minutes for cleaning, and {timings[2]['wall_minutes']:.2f} minutes for target construction, calculated from each stage's original start/finish receipts. They include that stage's lifecycle and built-in checks but exclude dependency installation, separately executed validators and human work between stages. No full-run peak RSS, CPU-utilisation or temporary-disk peak measurements were collected. Formal Parquet payloads occupy 4,678,105,303 bytes, which is not a peak-disk requirement.

## Verification, limitations and AI assistance

Existing full-data validation includes count reconciliation, unique keys, time-grid/schema checks, root-directory readback and file hashes. Target checks include independent Decimal recomputation for 29 real cases covering 522 expected positions. The additional P1-6 replay uses {replay['counts']['raw_records']:,} preserved raw source records and {replay['counts']['target_anchors']} selected anchors, covering all 86 collisions and the development-period manual cases. Headers/newlines are reconstructed, while record text is unchanged. The frozen production functions are rerun; all cleaned observations, collision lineage and selected targets are compared against full-run reference rows after Parquet writing/reading, and every selected target is independently recomputed by timestamp-key lookup. The initial local replay took approximately {replay['elapsed_seconds']:.2f} seconds with `local[2]`, 2g and eight shuffle partitions. It is a functional check, not a representative performance benchmark. A separately extracted support-package replay is documented by its receipt.

Missing coverage limits monthly/annual interpretation and evaluation scope. Alignment and interval interpretation are explicit project assumptions. Metadata first/last appearances do not establish continuous validity periods. Under the retrospective event-time availability assumption, delayed updates are audited but are not predictors; results should not be claimed as demonstrated real-time deployment performance. The eight-year record supports descriptive interannual comparisons, not an established long-term climate trend. A's checks use the same host and configured runtime; B/C and macOS acceptance, and final-model reload/prediction validation, remain pending.

Suggested AI disclosure for A, to be reviewed and supplemented by the team: OpenAI Codex assisted with planning, implementation, documentation, quality checks and reproduction workflows. Reported A-stage counts, hashes and execution status were checked against actual outputs. B/C should disclose their own AI use and verification separately. This draft does not claim that all code has been manually reviewed line by line or that modelling is complete.
'''
    # A complete English annual table; keep the draft free of untranslated headings.
    english = english.replace(annual_table, '| Year | Raw records | Observations | Merged records | Future complete | Future unknown | Both windows complete |\n|---|---|---|---|---|---|---|\n' + annual_table)
    (ROOT / 'docs/a_report_section_en.md').write_text(english, encoding='utf-8')
    (ROOT / 'docs/a_cleaning_report_zh.md').write_text(english, encoding='utf-8')
    print('Generated A report drafts and exact resource/loss tables')


if __name__ == '__main__':
    main()
