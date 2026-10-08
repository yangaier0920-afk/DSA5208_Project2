# A数据工程：P1-6复现与报告支持

这是 `rainfall_v1` 的补充入口。正式清洗数据、规则配置与P1-5发布清单继续保持原哈希；本材料使用独立 `manifests/a_support_manifest.json` 记录支持版本和文件身份。执行状态查看 `configs/processing_status.json`；A本地验收查看 `outputs/p1_6_support/validation.json`。

本包可执行**真实原始记录片段 → 清洗／对齐／去重 → Parquet写出重读 → 未来累计量和窗口完整性 → 与正式参照逐列比较**。不需要下载完整8GB CSV，也不训练模型。

## 1. 文件导航

| 用途 | 文件／目录 |
|---|---|
| B/C正式数据接口、质量规则、版本约定 | `docs/data_contract.md`、`manifests/release_manifest.json` |
| A中文报告材料 | `docs/a_cleaning_report_zh.md` |
| 可合并到英文课程报告的A章节草稿 | `docs/a_report_section_en.md` |
| 计算环境、全量阶段耗时、年度数量和损失表 | `outputs/p1_6_support/report_metrics.json`、`full_run_resources.csv`、`annual_cleaning_and_targets.csv`、`loss_ledger.csv` |
| 本机硬件清单 | `outputs/p1_6_support/hardware_inventory.json` |
| 真实原始记录片段及正式参照 | `data/samples/p1_6_v1/` |
| 可重复运行入口 | `scripts/reproduce_a_sample.py` |
| A本地首次重跑输出 | `outputs/p1_6_support/local_replay_v1/` |
| P1-6支持清单与迁移验收 | `manifests/a_support_manifest.json`、`outputs/p1_6_support/validation.json` |

报告中的所有清洗与标签数量取自原始全量验收，不能把片段规模写成正式数据规模。英文稿尚需合并B/C分析、特征、模型结果以及全组姓名／学号／AI声明，不是最终课程PDF。

## 2. 环境准备

统一Python 3.11、Java 17、PySpark 3.5.7、Py4J 0.10.9.7，Spark时区Asia/Singapore。

在A当前项目使用已配置的 `.venv`，不用重装环境。若从支持ZIP解压到另一个目录，解压包中没有Java、虚拟环境或Windows native组件，需要先配置运行环境。Windows新机器在包根目录可执行：

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --index-url https://pypi.org/simple -r requirements-spark.txt
.\.venv\Scripts\python.exe scripts\setup_java17.py
.\.venv\Scripts\python.exe scripts\setup_hadoop_windows.py
```

Java与Windows组件下载入口及来源校验见 `docs/environment.md`。网络或安装问题按实际错误处理，不宣称安装流程已在B/C机器测试。

macOS准备：

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-spark.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

macOS须先安装Java 17，使用同一数据逻辑；不需要Windows native组件。当前macOS实机验收仍待成员执行。

## 3. 运行代表性数据复现

在项目或解压目录根目录，Windows执行：

```powershell
.\.venv\Scripts\python.exe scripts\reproduce_a_sample.py --output outputs/p1_6_runs/run_001
```

macOS执行：

```bash
python scripts/reproduce_a_sample.py --output outputs/p1_6_runs/run_001
```

如从其他工作目录运行，使用脚本和输出的绝对路径；脚本按自身位置识别项目根目录。每次选择新的输出目录，已有目录会被拒绝覆盖。首次A运行约73秒是功能检查的参考，不是其他机器的耗时保证。

成功条件：控制台显示 `PASS: raw-to-target replay`，退出码0，输出目录 `verification.json` 的status为PASS、全部checks为true、shutdown.passed为true。输出还包含observations、targets、逐源年度清洗账目和全部114个锚点的独立Decimal复算记录。

输入共1,297条真实原始整行，覆盖29个2017—2020开发期人工案例及全部86组2017对齐碰撞；跨年上下文延伸到2021，未利用2024标签选择案例。原始表头及换行重新构造；source_sha256指完整原始年度文件，fixture_sha256指当前片段，两者不能混用。片段中的缺失是原资料中的缺槽，所有选定锚点所需的实际历史／未来观测已纳入。

核验步骤：核对配置、原生产函数与片段哈希；调用已冻结的解析／碰撞函数；保留完整原始来源字段；写出重读观测；逐列比较正式观测和双侧碰撞参照；先在完整片段上下文生成窗口再选114个锚点；写后重读targets并逐列对比正式累计量、空值、完整性、角色；用独立timestamp-key/Decimal算法复算每个锚点。

此入口检验数据工程链条，不展开训练阈值、不生成B的学习型特征、不拟合或加载C的模型。它与P1-5每表1,309行的接口样本不同：P1-5验证读取接口，P1-6验证从原始片段重新加工的结果。

## 4. 正式大数据的复现边界

完整数据使用 `scripts/read_data_release.py --scope full --member A` 核对已取得的全量Parquet文件哈希、schema和唯一关联键；需先按发布清单恢复完整目录。该命令只读验证正式数据，不重新生成它。

从八年CSV全量重建应使用独立工作副本、新的输出／证据，并重新生成逐阶段验收和发布快照。历史全量入口为src/01_audit_raw.py、src/02_clean_standardize.py、src/03_build_targets.py及其根表／交付校验脚本，细节见对应阶段契约。旧入口有固定v1来源路径、阶段签名与状态写回依赖，不能仅替换 --source 或 --output 就当作任意目录的端到端runner，也不能在冻结底座上强制覆盖。P0-3旧校验会重写阶段状态，P0-4旧校验会重写历史报告；日常检查使用统一reader。

本次全量证据来自已经执行过的原始八年运行；新增运行是代表性片段的本机复现与迁移复现，不宣称完成第二次八年全量重建。原始CSV／正式大表约8.05GB／4.68GB，不在这个小支持ZIP内。GitHub与全量下载渠道在最终确认后处理。

## 5. 阶段完成与团队待办

A负责的本地报告材料、资源／损失记录、运行入口、真实片段复现和支持包核验以P1-6收据为准。另一成员／macOS实机复现、B/C结果版本标记、最终模型加载和同输入预测一致性仍需团队随后完成。A没有承担模型训练。

运行环境与资源口径：原全量任务local[4]、driver heap 4g、48 shuffle分区；片段复现local[2]、2g、8分区。Driver堆限额不是实际内存峰值；全量RSS／CPU占用／临时磁盘峰值未测量，报告明确留空而不是估计为配置值。运行耗时源于原收据，安装、额外交付校验和阶段间人工时间不计入各全量阶段。
