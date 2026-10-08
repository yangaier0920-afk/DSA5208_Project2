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
    draft = f'''# A负责的清洗、计算环境与复现报告材料

版本：{identity['release_id']}；data_version={identity['data_version']}；config_hash={identity['config_hash']}。本文件只覆盖A已执行的数据工程工作，可供团队合并报告；Task 1分析、历史特征、模型及评价结论由B/C补齐。英文可用稿见a_report_section_en.md。

## 数据来源与正式规模

使用data.gov.sg的2017—2024八个年度降雨CSV，五分钟间隔、源单位mm。八个源文件共8,047,044,701字节。官方数据页与本地全文SHA-256见manifests/raw_manifest.json；用户确认文件为官方导出且完整获取，这不等于已核对服务器端内容哈希。正式规模以Spark解析数48,538,713为准。

原始数据中结构、数值、单位／类型异常及原始键重复均未检出；存在2017非标准秒相位、全网63个整日资料缺口、站点覆盖与元数据变化。2018年8月4—31日没有记录，不能把缺口视作零降雨，也不能仅由文件推断物理原因。

## 清洗与时间标准化

保留原始12列文本、整行、源文件与记录哈希，以及原始event_ts。仅2017时间秒余数为299的记录增加1秒形成五分钟区间末端slot_end_ts。1,775,327条源记录触发对齐，保留表中1,775,241条仍带此对齐标记。

对齐产生86组同键且雨量一致的碰撞：优先保留原来就在标准网格上的记录，不累加也不平均；合并移除86条，双方172条记录保留在collision_lineage。两条保留观测存在元数据差异标记，历史元数据不以最新坐标覆盖。雨量不同的同键组按规则整组隔离，但本次真实数据未出现。

只保留有限且非负雨量，保留真实零与有效极端值，不进行任意截断或0.2mm舍入。标签累计使用Decimal(38,18)，源有效小数精度最高5位；无法精确表示时停止而不是静默舍入。存储18位小数不代表测量精度增加。雨量定义为截至slot_end_ts的五分钟区间量，Task 1日历归属按rain_period_start_ts=slot_end_ts-300秒。

数量账目：48,538,713 = 48,538,627正式观测 + 86合并移除 + 0隔离。合并比例为{86 / raw_count * 100:.8f}%。隔离为空不代表时间序列完整或资料没有质量风险。正式观测中47,121,647条雨量为零；全部保留，不对未知位置创建零记录。

## 未来累计量与历史完整性

每条观测对应一个预测锚点t。按站点和实际epoch检查t+300至t+1800的六个精确位置，全部有效才保存future_30m_mm；未知累计为null，已知无雨为Decimal零。历史完整性检查t-3300至t的12个位置。先读跨年度必要上下文，再选择本年度锚点，不采用“后六行”窗口。

共有48,538,627个锚点，47,636,833个未来完整、901,794个未来未知；47,347,833个历史完整、1,190,794个历史不完整。历史与未来同时完整的基础资格为47,048,872，仍需C按E2边界与已知站点支持范围筛选。历史失效和未来失效有重叠，不能直接相加。所有失效锚点保留在targets中，不算清洗删除。

| 年份 | 原始行数 | 保留观测 | 合并移除 | 未来完整 | 未来未知 | 历史和未来均完整 |
|---|---|---|---|---|---|---|
{annual_table}

课程标签为未来累计量严格大于给定阈值；相等不超阈，无效累计不能展开成负例。A没有进行阈值样本展开、特征学习或训练。E2角色用于2017—2020/2021、2017—2021/2022、2017—2022/2023三轮开发；锁定配置后2017—2023重训并对2024最终测试。目标或窗口信息不可进入模型特征或事前预测门槛。

## 计算环境、耗时与资源口径

本机{hardware['os']['Caption']}，{cpu}，{hardware['cpu']['NumberOfCores']}物理核心／{hardware['cpu']['NumberOfLogicalProcessors']}逻辑处理器，当前系统可见物理内存约{ram_gib:.2f}GiB。Python 3.11.5、Spark/PySpark 3.5.7、Py4J 0.10.9.7、Java 17.0.20.1；时区Asia/Singapore。全量运行local[4]、driver memory 4g、shuffle partitions 48；这是本地线程和JVM堆配置，不是独立四节点集群或进程总内存峰值。

| 全量阶段 | 起止收据耗时（分钟） | 年度计时合计（分钟） |
|---|---|---|
{timing_table}

耗时从各阶段原始run_status起止时间计算，包含相应阶段初始化、处理、内置核验与退出；不包含安装、另行执行的交付校验和阶段之间的人工工作。不同阶段的年度计时口径不同，不能据此作算法性能比较。全量峰值RSS、CPU利用率与临时盘峰值当时未采集，不写成4GB或零；当前正式Parquet合计4,678,105,303字节，也不是全流程最大磁盘需求。

本机Windows本地组件为固定提交的社区Hadoop 3.3.5，Spark自带Hadoop Java为3.3.4；本工作负载实际读写通过，组件来源与局限见docs/environment.md。硬件清单是在P1-6阶段读取，不是历史资源峰值测量。

## 复现验证与可复现范围

全量阶段已有年度检查、根表读回、计数闭合和正式文件哈希验收。P0-4另有30项合成测试、29个真实人工案例及522个预期槽复算。P1-5验证八年配对样本接口读写和同机新目录迁移。

P1-6新增真实原始片段重跑：{replay['counts']['raw_records']:,}条源记录，来自2017—2021必要上下文，覆盖29个开发期人工案例及全部86组碰撞，{replay['counts']['target_anchors']}个选定锚点。原始整行保留自已校验的observations/lineage，片段只重建表头和换行；不是重新下载的完整CSV。重用与正式版本相同且经哈希核对的解析、碰撞和窗口函数，写后重读，逐列与全量参照输出双向比较，并独立使用Python Decimal复算全部选定锚点。首次本地重跑约{replay['elapsed_seconds']:.2f}秒，local[2]、2g、8个shuffle分区；这是功能复现而非全量性能基准。

另以单独支持包进行解压迁移重跑，验收记录见outputs/p1_6_support/validation.json。A的本机测试不代表B/C或macOS已验证；最终模型尚未由C提供，模型加载与预测一致性仍是团队后续验收。完整复現需要另获八年CSV／正式大表并核对清单；小包只支持代表性数据流程。

## 局限与AI使用声明材料

缺测不填补，因此观测累计不是缺测月份的完整真实总雨量；Task 1采用覆盖面板且须报告可比较范围。2017+1秒与区间末端解释属于已确认的工程口径，应在报告中明确。元数据历史的first/last不是连续有效期，不能据此自动推断站点移动时间。本组采用回溯事件时间可用假设，较大的更新时间延迟意味着不能把历史结果直接宣称为实时在线部署效果。

站点异质性、极端值和潜在测量误差在资料中仍可能存在，零隔离不等于气候质量认证；八年变化只能支持该资料范围的年际描述。基于覆盖阈值、H1完整性和已知站点的下游筛选会改变可评价样本范围，B/C须另报告筛选损失。P1-6没有替代模型性能评估。

AI声明建议（由组员提交前核对并补充）：A使用OpenAI Codex辅助分工细化、代码与文档编写、质量检查及复现流程。A部分的运行状态、数量和哈希以实际输出核验；模型与分析的AI使用需B/C分别补充，不宣称整组已独立人工逐行审查或已完成所有实验。

机器可读表：outputs/p1_6_support/report_metrics.json、full_run_resources.csv、annual_cleaning_and_targets.csv、loss_ledger.csv。正式数据身份不变；本材料为A复现／报告支持补充，不修改rainfall_v1冻结文件。
'''
    (ROOT / 'docs/a_cleaning_report_zh.md').write_text(draft, encoding='utf-8')
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

{annual_table.replace('| 年份 |', '| Year |')}

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
    print('Generated A report drafts and exact resource/loss tables')


if __name__ == '__main__':
    main()
