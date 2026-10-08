# DSA5208 Project 2：本机 Spark 数据底座

当前统一交接入口是 [数据契约](docs/data_contract.md)、[完整字段字典](docs/field_dictionary.md) 与 `manifests/release_manifest.json`。交付身份为 `rainfall_v1`／`data_version=v1`，绑定 `p0_3_v1` 观测和 `p0_4_v1` 标签；B/C结果必须另存 `result_metadata.json` 标明数据配置及实验配置哈希。P1-5本地验收状态见 [交接说明](docs/p1_5_status.md) 和 `outputs/p1_5_release/validation.json`。

B/C先在项目根目录运行 `python scripts/read_data_release.py --member B`（C改成员），读取当前正式接口的配对样本；使用已经配置好的Python 3.11虚拟环境。Windows完整命令、macOS环境配置及全量读取入口见数据契约。本次接口ZIP为 `outputs/p1_5_release/rainfall_v1_interface.zip`，不含大型正式数据；GitHub发布等待最终内容确认，B/C实机验收仍按此前安排暂缓。

目前已完成本机PySpark最小读写验证、P0-2八年Spark全量质量审计、P0-3清洗与标准化及P0-4未来累计量／时间连续性验收。P0-3保留48,538,627条观测、隔离0条、合并移除86条重复；P0-4每条观测保留一个锚点。当前完成状态见 `configs/processing_status.json`，报告见 [清洗报告](docs/p0_3_quality_report.md) 与 [targets验收报告](docs/p0_4_quality_report.md)。阈值标签展开、B的历史特征及模型训练尚未执行。

全量核定 48,538,713 条记录、91 个站点，生成 6,528 行 station-month 覆盖表。审计报告见 [docs/p0_2_quality_report.md](docs/p0_2_quality_report.md)，结果目录为 `outputs/p0_2_audit/p0_2_v1/`。用户已确认文件为官方完整导出，2018 年 8 月 4—31 日等缺口按资料缺测处理。全部15项决定已确认，见 [docs/P0-3及后续决策选项.md](docs/P0-3及后续决策选项.md) 和 `configs/project.json`；E采用已确认的 [E2滚动验证实验方案](docs/E2滚动验证实验方案.md)。训练未开始。

P0-1 的本机交付已准备齐全，全组验收仍待 B/C 实机确认，见 [docs/p0_1_status.md](docs/p0_1_status.md)。原始清单位于 `manifests/raw_manifest.json`，环境版本约定位于 `configs/environment.lock.json`。

固定共享样本位于 `data/samples/p0_1_v1/`，交接 ZIP 为 `outputs/p0_1_shared_sample_v1.zip`；Windows/macOS 的读取操作见 [docs/shared_sample_readme.md](docs/shared_sample_readme.md)。共享包不包含原始八年 CSV，可单独进行读取验收。

## 在这台电脑重复验证

在项目根目录打开 PowerShell，执行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_pyspark.ps1
```

这只为该 PowerShell 进程指定执行策略，不修改系统策略。不需要手工激活 Python 环境或永久配置 Java 路径。

预期最后输出 `PASS: ...verification.json`，进程返回码为 0。每次生成独立的 `outputs/spark_smoke/<run_id>/`，其中包含：

- `sample.csv`：2017—2024 各 2,000 条原始记录，共 16,000 条。
- `parquet/year=.../month=.../`：本次 Spark 实际写出的分区文件。
- `verification.json`：版本、参数、检查结果、样本来源和耗时。

可用参数缩小或扩大年度样本：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_pyspark.ps1 --rows-per-year 2000
```

详细检查结果和 Windows 处理方法见 [docs/environment.md](docs/environment.md)。

## 其他成员创建同样的 Windows 环境

需要 64 位 Python 3.11。将项目代码及八个年度 CSV 放在自己的项目目录，联网执行：

```powershell
python -m venv .venv
$env:PYTHONIOENCODING = 'utf-8'
.\.venv\Scripts\python.exe -m pip install --index-url https://pypi.org/simple --cache-dir .runtime\pip-cache -r requirements-spark.txt
.\.venv\Scripts\python.exe scripts\setup_java17.py
.\.venv\Scripts\python.exe scripts\setup_hadoop_windows.py
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify_pyspark.ps1
```

Java 安装脚本下载锁定版本的 Microsoft 官方 ZIP 并核对官方 SHA-256。Windows Hadoop 安装脚本下载固定提交的社区构建，核对 Git blob 并记录 SHA-256。两者均保存到项目内的 `.runtime`。

`.venv` 应在每台电脑重新创建，不直接复制，也不作为最终源码提交内容。Linux/macOS 的 Java/Hadoop 配置方式不同，这里的安装脚本与最小验收入口针对 Windows。

## 运行后续 Spark 脚本

全量审计使用统一环境入口启动：

```powershell
.\.venv\Scripts\python.exe scripts\run_spark.py src\01_audit_raw.py
```

`src/01_audit_raw.py` 已实际执行通过，使用 `local[4]`、driver 4 GiB、48 个 shuffle 分区和 `Asia/Singapore` 时区。默认目录已有验收结果，重跑请指定新的 `--output`；中断续跑使用相同目录并加 `--resume`，会核对代码及输入签名。程序不修改原始 CSV。

先运行合成异常测试、重跑全量、补充核查时间刻度及校验交付的完整命令见 [docs/p0_2_methodology.md](docs/p0_2_methodology.md)。审计程序按年完成全表扫描和排序，在本机需要持续运行；检查点逐年保存。

## P0-3 清洗与统一Parquet

数据接口、原始溯源、精确小数、跨年分析口径与完整复现命令见 [docs/p0_3_data_contract.md](docs/p0_3_data_contract.md)。默认输出 `data/processed/p0_3_v1/`，包含observations、quarantine、collision_lineage及station_metadata_history；验收记录在 `outputs/p0_3_clean/p0_3_v1/`。

本机八年主清洗实测34.27分钟（另有合并读取与交付校验），使用既定local[4]/4GiB配置。正式目录包含120个Parquet文件，总计4,275,155,481字节（约4.28GB）；共享时保留year/month目录结构。21项合成检查、8年各9项重读检查、11项根目录接口检查及32项交付检查通过，357个交付文件的SHA-256位于 `outputs/p0_3_clean/p0_3_v1/output_manifest.json`。B/C其他电脑上的实际验收仍须各自执行，不据本机读取成功声称已在macOS验证。

```powershell
.\.venv\Scripts\python.exe scripts\run_spark.py src\02_clean_standardize.py
.\.venv\Scripts\python.exe scripts\run_spark.py src\02b_verify_parquet.py
.\.venv\Scripts\python.exe scripts\validate_p0_3_outputs.py
```

已有结果不自动覆盖；独立复现使用新的输出与验收目录，中断续跑加 `--resume` 并保持代码与配置不变。

## P0-4 真实累计量与窗口完整性

用户确认由A继续按原计划实施P0-4，现已完成八年全量生成与验收。48,538,627个锚点中，47,636,833个未来六槽完整，901,794个未来缺槽的累计量保持null；47,048,872个同时满足过去60分钟与未来30分钟完整性。30项合成检查、8年各15项验收、29个真实逐槽复算案例、19项合并目录检查及43项交付检查通过。275个交付文件已记录SHA-256。最终状态见 `configs/processing_status.json`，接口见 [docs/p0_4_data_contract.md](docs/p0_4_data_contract.md)，详细报告见 [docs/p0_4_quality_report.md](docs/p0_4_quality_report.md)。

```powershell
.\.venv\Scripts\python.exe scripts\run_spark.py src\03_build_targets.py
.\.venv\Scripts\python.exe scripts\run_spark.py src\03b_verify_targets.py
.\.venv\Scripts\python.exe scripts\validate_p0_4_outputs.py
```

默认数据在 `data/processed/p0_4_v1/targets/`，证据在 `outputs/p0_4_targets/p0_4_v1/`。一条观测对应一个锚点；缺未来槽则累计量为null，历史不足另行标记。E2角色分别保存为fold1_role、fold2_role、fold3_role、final_role，不用一个固定split替代滚动用途。

本阶段不训练模型、不展开阈值、不计算B的历史特征。C使用相应role与model_anchor_eligible筛选，再实施采样、阈值展开和训练；未来真实累计量不能进入特征向量。P0-4之后不要直接用旧P0-3校验脚本重写汇总阶段状态；输入签名由P0-4核对，旧收据保留。

如果使用 VS Code，可将解释器选为项目 `.venv\Scripts\python.exe`；涉及 Spark 时仍通过上述入口配置 Java、Hadoop 和路径编码。

当前八年预检查和后续分工详见 [A成员工作细化与数据底座方案.md](A成员工作细化与数据底座方案.md)。
