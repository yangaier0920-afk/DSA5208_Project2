# 降雨项目：组员从这里开始

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
.\.venv\Scripts\python.exe -m pip install -r requirements-spark.txt
.\.venv\Scripts\python.exe scripts\setup_java17.py
.\.venv\Scripts\python.exe scripts\setup_hadoop_windows.py
```

**Windows读取检查**（B执行，C将B改成C）：

```powershell
.\.venv\Scripts\python.exe scripts\read_data_release.py --member B
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
.\.venv\Scripts\python.exe scripts\record_result_metadata.py --member B --run-id features_001 --input-scope full --result-dir outputs/B/features_001 --experiment-config configs/B_features.json --command "python src/B_features.py"
```

C改成员与运行名；macOS把Python入口改成`python`。请把自己的实验配置随结果保留，不修改A的project.json。接口样本产生的测试结果应注明`--input-scope sample`。

## 5. A的复现与当前待办

A的真实原始片段重跑入口（可选；B/C正常使用数据不必先跑）：

```powershell
.\.venv\Scripts\python.exe scripts\reproduce_a_sample.py --output outputs/reproduction/run_001
```

每次换一个新输出目录；macOS用`python`。该测试重跑1,297条真实片段、114个锚点，验证清洗和标签，与正式参照比较；不训练模型。环境、清洗报告中英文稿、局限和189个字段定义全部放在补充资料。

当前A本地数据工作和复现通过；B/C实机验收、分析／特征／模型、最终模型重载、全组报告合并及最终发布仍待完成。`archive/`保存历史说明和收据，日常使用以上入口即可；其他阶段脚本是A的内部处理／归档工具，不是B/C必跑步骤。
