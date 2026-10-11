# 降雨项目：组员从这里开始

A已完成八年审计、清洗、未来累计量及本地复现。**B/C直接使用下面的正式Parquet，不用重新清洗原始CSV。** 日常只需本README；需要字段、规则或报告文字时查[补充资料](docs/reference.md)。

## 1. 下载正式数据（B/C先从这里开始）

**[打开正式数据下载页：2017—2024八年 Parquet](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/tag/rainfall_v1_r2)**

1. 先[下载最新版项目代码ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/archive/refs/heads/main.zip)并解压，也可以clone仓库。
2. 下载下表**全部8个年度包**。合计约2.70GB，解压后的正式数据约4.68GB；保留下载包和解压数据时，数据部分需约7.4GB空间。
3. 在代码中`README.md`所在目录打开终端，把8个包解压到该目录，保留`data/processed/`下的路径。每包包含对应年份的观测、标签、异常隔离、重复追溯和站点元数据。

| 年份 | 下载链接 | 压缩包大小（约） |
|---|---|---|
| 2017 | [下载ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/download/rainfall_v1_r2/rainfall_v1_2017.zip) | 295 MB |
| 2018 | [下载ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/download/rainfall_v1_r2/rainfall_v1_2018.zip) | 253 MB |
| 2019 | [下载ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/download/rainfall_v1_r2/rainfall_v1_2019.zip) | 277 MB |
| 2020 | [下载ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/download/rainfall_v1_r2/rainfall_v1_2020.zip) | 363 MB |
| 2021 | [下载ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/download/rainfall_v1_r2/rainfall_v1_2021.zip) | 391 MB |
| 2022 | [下载ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/download/rainfall_v1_r2/rainfall_v1_2022.zip) | 401 MB |
| 2023 | [下载ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/download/rainfall_v1_r2/rainfall_v1_2023.zip) | 373 MB |
| 2024 | [下载ZIP](https://github.com/yangaier0920-afk/DSA5208_Project2/releases/download/rainfall_v1_r2/rainfall_v1_2024.zip) | 350 MB |

**Windows解压**（假设8个ZIP保存在默认Downloads目录）：

```powershell
2017..2024 | ForEach-Object { Expand-Archive -LiteralPath (Join-Path $HOME "Downloads\rainfall_v1_$_.zip") -DestinationPath . -Force }
```

**macOS解压**（同样假设ZIP保存在Downloads目录）：

```bash
for year in {2017..2024}; do unzip -o "$HOME/Downloads/rainfall_v1_${year}.zip" -d .; done
```

若ZIP存在其他目录，替换命令中的Downloads路径。解压后应看到`data/processed/p0_3_v1/`和`data/processed/p0_4_v1/`，然后按第3节安装环境并检查读取。仓库代码与交接小样本包用于代码／接口准备，正式分析和训练使用这8个年度包。

## 2. 交付给B/C的是什么

| 文件／目录 | 用途 |
|---|---|
| `data/processed/p0_3_v1/observations/` | 正式观测，48,538,627行；B做降雨分析、历史特征 |
| `data/processed/p0_4_v1/targets/` | 每条观测对应一个锚点；C取未来30分钟实际累计量、完整性和E2用途 |
| `outputs/p0_3_clean/p0_3_v1/` | B所需覆盖表及跨年可比较站点面板；另有清洗质量证据 |
| `configs/project.json` + `manifests/release_manifest.json` | 固定规则、数据版本、schema、文件哈希；请勿各自修改 |
| `data/samples/p1_5_v1/` | 环境／接口检查样本，每张表1,309行；不用于正式训练或结论 |

数据版本为`v1`，观测表版本`p0_3_v1`，标签表版本`p0_4_v1`；当前交接说明修订为`rainfall_v1_r3`。正式数据包使用已发布的`rainfall_v1_r2`下载，Parquet内容、字段、表路径与规则完全一致。

## 3. 安装环境与读取检查

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

看到`PASS: sample`且退出码0即通过；本机收据在`outputs/p1_5_receipts/B_sample.json`。复跑时加`--receipt outputs/p1_5_receipts/B_run2.json`选择新收据，避免覆盖。下载并解压8个年度包后，再执行**全量读取检查**，耗时及资源需求更高：

```powershell
# Windows，B执行；C把成员和收据文件名中的B改成C
.\.venv\Scripts\python.exe scripts\read_data_release.py --member B --scope full --receipt outputs/p1_5_receipts/B_full_r3.json
```

```bash
# macOS，先激活上面的虚拟环境
python scripts/read_data_release.py --member B --scope full --receipt outputs/p1_5_receipts/B_full_r3.json
```

看到`PASS: full`且退出码0即通过。每次复跑选择新的收据文件名。

## 4. B/C各自下一步

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

## 5. 结果如何注明版本

每份特征、图表来源表、模型或评价结果保存`result_metadata.json`，记录`data_version`、`config_hash`、发布标识和实验参数哈希。已有实际结果和实验配置后，可用下面的入口；示例路径需替换成自己的真实文件：

```powershell
.\.venv\Scripts\python.exe scripts\record_result_metadata.py --member B --run-id features_001 --input-scope full --result-dir outputs/B/features_001 --experiment-config configs/B_features.json --command "python src/B_features.py"
```

C改成员与运行名；macOS把Python入口改成`python`。请把自己的实验配置随结果保留，不修改A的project.json。接口样本产生的测试结果应注明`--input-scope sample`。

## 6. A的复现与当前待办

A的真实原始片段重跑入口（可选；B/C正常使用数据不必先跑）：

```powershell
.\.venv\Scripts\python.exe scripts\reproduce_a_sample.py --output outputs/reproduction/run_001
```

每次换一个新输出目录；macOS用`python`。该测试重跑1,297条真实片段、114个锚点，验证清洗和标签，与正式参照比较；不训练模型。环境、清洗报告中英文稿、局限和189个字段定义全部放在补充资料。

A本地数据工作、复现和正式数据发布已完成；B/C实机验收、分析／特征／模型、最终模型重载、全组报告合并及课程最终提交仍待完成。`archive/`保存历史说明和收据，日常使用以上入口即可；其他阶段脚本是A的内部处理／归档工具，不是B/C必跑步骤。

## 7. B：Task 1 分析与历史特征交付

B 已使用 2017—2024 年正式全量数据完成 Task 1 分析、历史特征构建及验证。以下为截至 2026-10-11 的 B 交付状态；C 的模型训练、实际训练输入关联核验及全组最终提交仍需完成。B 的代码与结果放在独立目录 `B_delivery/`，沿用 A 的冻结数据发布规则。

### 7.1 文件位置

以下路径均相对于项目根目录。

| 路径 | 内容 |
|---|---|
| `B_delivery/notebooks/B_Task1_Analysis.ipynb` | 可编辑的 Task 1 聚合、七组图表及分析说明 |
| `B_delivery/notebooks/B_Historical_Features.ipynb` | 可编辑的历史特征、逐年验证及关联核验函数 |
| `B_delivery/src/` | Spark 分析、特征生成、预测接口、验证和报告生成脚本 |
| `B_delivery/configs/B_experiment.json` | B 实验配置；当前数值特征版本为 `B_v2` |
| `B_delivery/results/task1_v2/` | 统计表、七组 PNG/PDF 图、覆盖门槛敏感性及来源记录 |
| `B_delivery/results/features_v2/` | 特征 schema、逐年保留率和验证记录；`parquet/` 为完整特征数据 |
| `B_delivery/results/join_audit/` | A 标签表与 B 特征表的逐年实际关联核验 |
| `B_delivery/docs/TIME_CONTRACT.md` | 预测时刻、五分钟区间、未来六槽及待共同确认事项 |
| `B_delivery/docs/FEATURE_DEFINITIONS.md` | 26 个数值特征的名称、含义、单位、范围和缺失处理；另有 CSV 版 |
| `B_delivery/C_HANDOFF.md` | C 的读取、关联、特征复用与防泄漏说明 |
| `B_delivery/report_zh/B_report_zh.md` | 唯一保留的中文 B 报告，可复制修改后合并进组报告 |

`task1_v2` 和 `features_v2` 是当前使用目录，不是旧版备份。报告引用的图位于 `results/task1_v2/figures/`；报告来源及哈希记录位于 `results/report_metadata/`。

### 7.2 环境与 Notebook 打开方法

先按前文完成 Python 3.11、Java 17、正式数据下载及全量读取验收。下面为 macOS 命令，在项目根目录执行：

```bash
source .venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
python -m pip install -r B_delivery/requirements-B.txt
python -m pip install notebook
python -m notebook
```

浏览器进入 `B_delivery/notebooks/`，打开所需 Notebook。默认重算开关为 `False`，Run All 用于查看已有结果，不启动全量 Spark。修改后需要重算时，打开对应开关并按从上到下的顺序运行；Notebook 使用带时间戳的新结果目录。不要同时运行两个全量 Spark Notebook。

已验证的本机环境为 MacBook Air、Apple M5、24 GB 内存，Python 3.11.9、PySpark 3.5.7、Java 17。Spark 使用 `local[2]`、4g JVM heap、128 个 shuffle 分区和 32 MiB 读取分区上限，启用 AQE 但禁用分区合并，不缓存全量观测。硬件清单为运行后补查；峰值内存未测量，详细计时范围见中文报告。

### 7.3 脚本复跑方法

以下命令仍在项目根目录执行。`task1_rerun1`、`features_rerun1` 必须是新的输出目录；再次运行时更换后缀，不能直接覆盖已有正式结果。

```bash
# Task 1：重新聚合并生成七组图
python B_delivery/src/B_task1_analysis.py --root "$PWD" \
  --output B_delivery/results/task1_rerun1
python B_delivery/src/B_plot_results.py \
  --input B_delivery/results/task1_rerun1

# 全量历史特征：生成并验证预测接口
python B_delivery/src/B_build_features.py --root "$PWD" \
  --output B_delivery/results/features_rerun1
python B_delivery/src/B_validate_feature_api.py --root "$PWD" \
  --features B_delivery/results/features_rerun1

# 重新核对标签与新特征的一对一关联
python B_delivery/src/B_join_audit.py --root "$PWD" \
  --features B_delivery/results/features_rerun1 \
  --output B_delivery/results/join_audit_rerun1
```

只需刷新当前正式中文报告和输出清单时：

```bash
python B_delivery/src/B_make_report.py
python B_delivery/src/B_finalize.py --root "$PWD"
```

以上报告命令读取当前正式的 `task1_v2`、`features_v2` 和 `join_audit` 结果，仅生成 Markdown。新一轮实验应另行指定其报告输入；关联证据也必须对应同一轮特征，不能直接沿用当前正式核验结论。

小数据边界检查可单独运行：

```bash
python B_delivery/src/B_test_features.py --root "$PWD"
python B_delivery/src/B_test_join_audit.py --root "$PWD"
```

### 7.4 分析与特征口径

- **Task 1**：按新加坡时区的 `rain_period_start_ts` 归属年月、日期和小时。严格跨年比较使用八年同月固定站点交集，以 90% 月覆盖为主、95% 为敏感性分析；当前仅 5、9、10、12 月受支持。年度摘要保留缺测和站点组成变化的限制，不解释为全国年度趋势。
- **时间口径**：按项目 L1/D1 约定，对齐后的 `slot_end_ts` 是五分钟区间终点。预测时刻 t 的当前槽对应 `(t−5min,t]`；未来30分钟标签使用末端 t+5 至 t+30 分钟的六槽，不包含当前槽。区间含义及 t 时刻当前观测已可用属于项目假设，尚未独立验证官方语义或真实发布延迟，A/B/C 仍需共同核对最终实现。
- **历史特征**：使用 t 及以前的精确时间位置，保留跨年上下文。默认数值输入为 26 列，清单以 `B_feature_api.NUMERIC_FEATURES` 为准；另由 C 加入阈值输入和仅在训练集拟合的站点编码。
- **缺失与截断**：缺槽保持 null，不补零。连续湿槽最多观察12槽，全湿时带左截断标记；未观察到近期雨时，原始距上次降雨时间未知，模型使用60截断编码及配套标记，不能解释为真实间隔。完整30分钟全干时雨量比为0，缺槽时为null。
- **历史筛选**：B 保留全部观测锚点，完整历史模型按规则排除历史不完整锚点。总体历史完整率约97.55%，2018年约87.68%；逐年数量见 `history_retention_by_year.csv`。未来完整性、role、边界等还会进一步影响监督样本，不能把历史保留率当最终训练保留率。

### 7.5 已完成验证与 C 的交接要求

正式特征共 **48,538,627 行**，其中 **47,347,833 行**具有完整60分钟历史。逐年数量与 A 核对通过；513 个真实锚点独立时间戳核验、六个合成边界案例及预测接口与离线26列一致性检查通过。

A 正式标签表与 B 特征表的主键均为 `station_id + prediction_epoch_s`。全量实际核验显示两侧主键唯一、匹配率100%、未匹配数0，关联前后没有增加样本。

**该基础表核验不能替代 C 实际训练输入的核验。** C 应在其筛选/采样完成后、阈值展开之前调用 `B_join_audit.audit_anchor_join`，保存本轮结果并检查两侧唯一性、匹配率、未匹配数及关联前后行数。阈值展开后改查 `station_id + prediction_epoch_s + threshold_mm` 三列键及每锚点阈值数量。

C 还需遵循 E2 滚动验证，仅在训练集拟合编码与预处理，并在锁定配置后重训、对2024进行最终评价。预测时可复用 `B_feature_api.features_at`；当前接口要求完整历史，不以未来标签是否存在作为事前预测条件。新增特征是否改善概率预测仍需 C 验证，B 未提供或宣称模型评价分数。


