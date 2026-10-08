"""Consolidate the team entry point while preserving the previous local release."""
import copy
from pathlib import Path
from data_release import ROOT, read_json, resolve_path, sha256, verify_release, write_json


README = '''# 降雨项目：组员从这里开始

A已完成八年审计、清洗、未来累计量及本地复现。**B/C直接使用下面的正式Parquet，不用重新清洗原始CSV。** 日常只需本README；需要字段、规则或报告文字时查[补充资料](docs/reference.md)。

## 1. 交付给B/C的是什么

| 文件／目录 | 用途 |
|---|---|
| `data/processed/p0_3_v1/observations/` | 正式观测，48,538,627行；B做降雨分析、历史特征 |
| `data/processed/p0_4_v1/targets/` | 每条观测对应一个锚点；C取未来30分钟实际累计量、完整性和E2用途 |
| `outputs/p0_3_clean/p0_3_v1/` | B所需覆盖表及跨年可比较站点面板；另有清洗质量证据 |
| `configs/project.json` + `manifests/release_manifest.json` | 固定规则、数据版本、schema、文件哈希；请勿各自修改 |
| `data/samples/p1_5_v1/` | 环境／接口检查样本，每张表1,309行；不用于正式训练或结论 |

数据版本为`v1`，观测表版本`p0_3_v1`，标签表版本`p0_4_v1`；当前交接发布标识`rainfall_v1_r2`仅更新说明组织。样本包没有约4.68GB的正式大表，正式分析／训练须另外取得大表并保持上表路径。GitHub及下载链接等待最终确认，目前尚未发布。

## 2. 先确认本机能读取

在本README所在目录打开终端。A现有环境可直接使用；新电脑只需首次安装。

**Windows首次安装**（已安装Python 3.11）：

```powershell
py -3.11 -m venv .venv
.\\.venv\\Scripts\\python.exe -m pip install -r requirements-spark.txt
.\\.venv\\Scripts\\python.exe scripts\\setup_java17.py
.\\.venv\\Scripts\\python.exe scripts\\setup_hadoop_windows.py
```

**Windows读取检查**（B执行，C将B改成C）：

```powershell
.\\.venv\\Scripts\\python.exe scripts\\read_data_release.py --member B
```

**macOS首次安装与检查**（先安装Python 3.11和Java 17）：

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-spark.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
python scripts/read_data_release.py --member B
```

看到`PASS: sample`且退出码0即通过；本机收据在`outputs/p1_5_receipts/B_sample.json`。复跑时加`--receipt outputs/p1_5_receipts/B_run2.json`选择新收据，避免覆盖。取得全部正式Parquet后可加`--scope full`校验全表，耗时及资源需求更高。

## 3. B/C各自下一步

| 成员 | 操作 |
|---|---|
| B | 用observations和覆盖表做Task 1；按截至t的历史观测生成特征，保存唯一的`station_id + prediction_epoch_s`键 |
| C | 用targets按当前轮role和`model_anchor_eligible`筛监督样本，再一对一关联B的特征；展开阈值并做E2训练／评价 |

在自己的Spark脚本中读取（示例脚本放在根目录下一级，如`src/`）：

```python
import sys
from pathlib import Path
root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / 'scripts'))
from data_release import read_tables, provenance

tables = read_tables(spark, root, scope='full')
observations, targets = tables['observations'], tables['targets']
data_identity = provenance(root)
```

`spark`是你自己的SparkSession；读取函数会设置新加坡时区并校验发布文件。观测用`slot_end_epoch_s`，标签用`prediction_epoch_s`，二者与station_id一起关联，不能仅按站点或日期关联。

必须记住：雨量单位mm；缺测不是0；标签严格比较`future_30m_mm > threshold_mm`，累计量为null不能变成负例。未来累计量、未来缺测及含未来信息的资格字段不可进入模型特征或事前预测门槛。Task 1按`rain_period_start_ts`归属日历，跨年比较使用覆盖合格的同月站点交集。完整规则和E2分区见补充资料。

## 4. 结果如何注明版本

每份特征、图表来源表、模型或评价结果保存`result_metadata.json`，记录`data_version`、`config_hash`、发布标识和实验参数哈希。已有实际结果和实验配置后，可用下面的入口；示例路径需替换成自己的真实文件：

```powershell
.\\.venv\\Scripts\\python.exe scripts\\record_result_metadata.py --member B --run-id features_001 --input-scope full --result-dir outputs/B/features_001 --experiment-config configs/B_features.json --command "python src/B_features.py"
```

C改成员与运行名；macOS把Python入口改成`python`。请把自己的实验配置随结果保留，不修改A的project.json。接口样本产生的测试结果应注明`--input-scope sample`。

## 5. A的复现与当前待办

A的真实原始片段重跑入口（可选；B/C正常使用数据不必先跑）：

```powershell
.\\.venv\\Scripts\\python.exe scripts\\reproduce_a_sample.py --output outputs/reproduction/run_001
```

每次换一个新输出目录；macOS用`python`。该测试重跑1,297条真实片段、114个锚点，验证清洗和标签，与正式参照比较；不训练模型。环境、清洗报告中英文稿、局限和189个字段定义全部放在补充资料。

当前A本地数据工作和复现通过；B/C实机验收、分析／特征／模型、最终模型重载、全组报告合并及最终发布仍待完成。`archive/`保存历史说明和收据，日常使用以上入口即可；其他阶段脚本是A的内部处理／归档工具，不是B/C必跑步骤。
'''


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
    moves += [(ROOT / n, archive / n) for n in ['README.md', 'README_A_P1_6.md', 'project_requirement.md', '项目分工.md', 'A成员工作细化与数据底座方案.md']]
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
    ref = '''# 项目补充资料（查阅用）

日常操作只看[README](../README.md)。本文件集中保存字段、规则、报告与复现细节。数据版本v1与配置保持不变；rainfall_v1_r2是说明整理后的交接修订。archive/history_v1保存原始阶段说明、原课程要求与上一版清单，供追溯。

目录：

1. [课程与分工](#课程与分工)
2. [数据和标签规则](#数据和标签规则)
3. [E2实验边界](#e2实验边界)
4. [完整字段字典](#完整字段字典)
5. [A中文报告材料](#a中文报告材料)
6. [A英文报告章节](#a英文报告章节)
7. [复现与证据](#复现与证据)
8. [课程原文](#课程原文)

## 课程与分工

2017—2024八年数据，用Apache Spark完成年内／跨年降雨分析，以及未来30分钟累计雨量严格超过输入阈值的概率预测，模型评价使用Spark MLlib。提交完整代码、运行README、训练好的模型及含全组姓名学号的PDF报告，并说明环境、方法、结果、局限和AI使用。课程截止日为2026-10-18。

A负责数据工程及复现／报告支持；B负责Task 1和历史特征；C负责阈值展开、模型、评价与预测入口。最终全组交叉复现、报告合并和提交共同完成。H1、E2、覆盖90%和采样比例是本组选择，不是课程新增强制规定。

## 数据和标签规则

| 规则 | 确定口径 |
|---|---|
| 时间与单位 | Asia/Singapore；epoch为秒，雨量为mm；slot_end_ts为5分钟区间末端 |
| 2017对齐 | 仅源年份2017且epoch mod 300=299时+1秒；原始event_ts和文本保留 |
| 碰撞 | 同键雨量相等保留原标准网格行，两侧留lineage；不等则整组隔离。保留原站点ID，包括S113 |
| 数值与缺测 | 保留有限非负值、真实零和极端值；不补缺测，不作0.2mm舍入；精确累计Decimal(38,18) |
| Task 1日历 | 用区间起点rain_period_start_ts；00:00末端属于前一天。裁剪年月前纳入边界上下文 |
| Task 1面板 | 38共同候选站，同月跨年比较使用覆盖≥90%的合格站点交集，95%敏感性，站点等权；不施加到全部观测／Task 2 |
| 未来标签 | t+300、600、900、1200、1500、1800六槽均有效才累计；未知为null，已知无雨为0；严格>阈值 |
| 历史完整性 | H1为t-3300至t每300秒共12槽；B另算历史特征 |
| 时效与元数据 | 回溯事件时间可用性假设；更新延迟只审计，不作为特征；不回填未来最新坐标，元数据first/last不代表连续生效区间 |

主键：observations为station_id + slot_end_epoch_s，targets为station_id + prediction_epoch_s，一对一关联。正式两表均48,538,627行。辅助表quarantine为空但有schema；collision_lineage含172行；station_metadata_history汇总合并前有效候选组合。未来有效47,636,833，未来未知901,794；历史与未来均完整47,048,872，再按轮次边界和已知站点范围筛选。

## E2实验边界

| 字段 | TRAIN | 验证／测试 |
|---|---|---|
| fold1_role | 2017—2020 | 2021 VALIDATION |
| fold2_role | 2017—2021 | 2022 VALIDATION |
| fold3_role | 2017—2022 | 2023 VALIDATION |
| final_role | 配置锁定后2017—2023重训 | 2024 TEST，仅最终评估 |

监督样本按对应role及model_anchor_eligible筛选；EXCLUDED_BOUNDARY排除，UNUSED不参与该轮。标签末端等于段右边界也排除；训练段内部跨日历年仍可用，不能按year_boundary_safe统一删除。实际预测只检查已有历史、站点、时间网格及阈值支持范围，不要求未来资料已经存在。

阈值候选0、0.5、1、2、5mm由C仅用最早训练段2017—2020确定支持范围并冻结。按阈值等权，再按三年验证年等权平均Brier选择配置。历史特征的编码／标准化按各轮训练数据拟合。1%流程验证与10%正式训练锚点采样先于阈值展开，验证／测试尽量全量；这些由B/C实施，A未训练模型。

future_30m_mm、future_n_valid、future_missing_offsets_s、target_valid、invalid_reason、model_anchor_eligible及含未来信息的边界字段不可作为模型特征或事前预测门槛。未知累计不能展开成负例。

## 完整字段字典

<details>
<summary>展开五张表的189个字段</summary>

'''
    dictionary = docs['field_dictionary.md'].split('\n', 2)[2]
    dictionary = dictionary.replace('见data_contract', '见本文件的数据和标签规则')
    ref += dictionary + '\n</details>\n\n## A中文报告材料\n\n'
    report = docs['a_cleaning_report_zh.md'].split('## 数据来源与正式规模', 1)[1]
    report = ('### 数据来源与正式规模' + report).replace('\n## ', '\n### ')
    report = report.replace('docs/environment.md', 'archive/history_v1/docs/environment.md').replace('英文可用稿见a_report_section_en.md', '英文稿见本文件下一节')
    ref += report + '\n\n## A英文报告章节\n\n'
    english = docs['a_report_section_en.md'].split('## Data and cleaning', 1)[1]
    ref += ('### Data and cleaning' + english).replace('\n## ', '\n### ')
    ref += '''

## 复现与证据

日常有三个入口：read_data_release检查读写接口，reproduce_a_sample重跑真实原始片段，record_result_metadata记录实际下游结果来源。首次两项A本机验证通过，不代表B/C／macOS已经完成实机验收。P1-5接口样本不是训练代表样本；P1-6片段包含114个选定锚点所需的实际上下文，缺槽保留。

原始全量质量证据在outputs/p0_2_audit/p0_2_v1、outputs/p0_3_clean/p0_3_v1、outputs/p0_4_targets/p0_4_v1；A资源与数量表在outputs/p1_6_support。原收据保留原发布标识，本次交接修订的验证在outputs/team_handoff。processing_status是当前进度，release_manifest是当前交付身份；不要运行旧阶段发布脚本覆盖当前说明和进度。

旧的全量入口和校验脚本有固定v1路径及签名依赖。完整重建须在独立工作副本逐阶段生成新证据及清单；仅改--source/--output不是通用全量runner。当前正式CSV／Parquet不在小包中，获取完整数据后保持README路径。GitHub未发布，最终下载渠道和课程提交包还需确认。

data_version只在数据内容／口径变化时提升；本次只修订文档发布标识。config_hash是project.json原始字节SHA-256，实验配置另存experiment_config_hash。新报告、特征、模型记录当前发布标识，旧结果保留其原记录，不静默替换。

## 课程原文

<details>
<summary>展开原课程要求</summary>

'''
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
