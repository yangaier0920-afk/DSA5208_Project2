# 本机 PySpark 环境与最小读写验证

验证日期：2026-10-08。结果：**PASS**。

## 已配置的环境

| 项目 | 实际版本/设置 |
|---|---|
| 操作系统 | Windows 11，x64 |
| Python | 3.11.5；项目 `.venv`，基于本机 Anaconda Python 创建 |
| PySpark / Spark | 3.5.7 |
| Py4J | 0.10.9.7 |
| Java | Microsoft OpenJDK 17.0.20.1 |
| Spark 自带 Hadoop Java 组件 | 3.3.4 |
| Windows Hadoop 本地组件 | cdarlint/winutils 的 3.3.5 构建，固定提交 `7386986d5d8a079b5cd4464f4599766dd27e7d13` |
| 运行模式 | `local[2]`，两个本机计算线程 |
| Driver memory | 2 GiB，适用于本次小样本验证 |
| Spark 时区 | `Asia/Singapore` |
| Shuffle partitions | 4，适用于本次小样本验证 |
| Python worker | 使用同一 `.venv` 下的 Python |

依赖位于本项目 `.venv` 和 `.runtime`。运行入口为当前进程配置 JAVA_HOME、HADOOP_HOME、SPARK_HOME 和 Python worker，未修改系统环境变量或现有 Java 20。

PySpark 3.5.7 官方支持 Python 3.8+ 和 Java 8/11/17。[安装说明](https://spark.apache.org/docs/3.5.7/api/python/getting_started/install.html)

Java ZIP 来自 [Microsoft 官方下载](https://learn.microsoft.com/en-us/java/openjdk/download)，已与官方发布的 SHA-256 核对。原生 Hadoop 组件是 [cdarlint/winutils 社区构建](https://github.com/cdarlint/winutils)，不是 Apache 官方二进制发行；已核对固定提交中的 Git blob 并记录 SHA-256。Apache 文档说明 Windows 本地文件系统操作需要 HADOOP.DLL 和 WINUTILS.EXE。[Windows 说明](https://cwiki.apache.org/confluence/spaces/HADOOP2/pages/120730292/WindowsProblems)

Spark 的 Hadoop Java 版本与 Windows 组件分别是 3.3.4 和 3.3.5，本次实际读写与 native loader 检查通过；这不是对其他 Hadoop/HDFS 工作负载的兼容性保证。组件来源与校验值分别保存于 `.runtime/java_manifest.json`、`.runtime/hadoop_manifest.json`。

本次 pip 使用本机已配置的清华 PyPI 镜像安装锁定版本。README 的新环境安装示例显式使用 PyPI 官方索引。

## 实际执行的检查

最终验收运行：`20261008_134521_945643`。

每个年度 CSV 取开头 2,000 行，合计 16,000 行，写成独立样本后由 Spark 读取。原始 CSV 仅读取，样本及 Parquet 位于 outputs。样本是环境测试输入，不是代表性天气样本。

| 检查 | 结果 |
|---|---|
| 八年 CSV 字段一致，显式 schema 读取 | 16,000 行，与独立 Python 计数一致 |
| ISO 8601 时间与雨量解析 | 样本解析失败为 0；本地日期与原 date 一致 |
| 原始秒数保留 | `2017-01-01T08:04:59+08:00` 对应 epoch 秒 1483229099 |
| 年份×站点聚合 | 473 组计数与雨量和，逐组对应独立 Python 计算 |
| 按 year/month 写入 Parquet | 8 个 Parquet 文件，合计 89,402 bytes |
| Parquet 重新读取 | 16,000 行，双向 `exceptAll` 差异均为 0 |
| Python worker | 两个分区实际计算，结果 210，与预期一致 |
| Hadoop native loader | 本地库成功加载 |
| Spark 会话与网关退出 | 正常退出，exit code 0，无退出错误 |
| Python 依赖检查 | `pip check`：No broken requirements found |

Spark 验证运行耗时约 28.3 秒，不含依赖下载与安装。这是小样本环境耗时，不能外推为全量处理耗时或空间需求。

机器可读证据：`outputs/spark_smoke/20261008_134521_945643/verification.json`。`outputs/spark_smoke/latest_pass.json` 指向最近成功结果；后续失败不会覆盖这份指针，因此复跑时仍需查看本次控制台 PASS/FAIL 与对应报告。

## 本次解决的 Windows 问题

1. 中文项目路径下，CMD 与 Java/Python 输出编码不同会导致 Java gateway 启动失败。运行入口显式设置 SPARK_HOME，并在运行期间统一为 UTF-8，退出时恢复控制台代码页。
2. 将中文文件路径转换成百分号编码的 URI 后，Spark/Hadoop 在这次环境中把编码文字当成路径。本地 Spark 输入输出使用正常的斜杠路径。
3. 缺少 winutils.exe 时，CSV 读取可通过，但 Parquet 目录创建失败。项目内补齐 winutils.exe/hadoop.dll 并配置 HADOOP_HOME 和 PATH 后，实际写出与重读通过。
4. Windows 退出时重复终止进程会出现 taskkill 报错。验证脚本先停止会话、关闭网关并等待本次进程退出，确认退出后取消该网关的备用退出回调。

## 后续边界

本次完成的是 A 工作包 P0-1 的本机环境与最小读写验收。没有进行全量清洗、缺失修补、重复消解、未来标签、模型训练或内存压力测试。

P0-1 补充交付已整理为 `manifests/raw_manifest.json`、`configs/environment.lock.json` 和固定共享样本 `data/samples/p0_1_v1/`。A 在交付副本上读取验收通过；B/C 及 macOS 实机结果仍待确认。全组状态详见 `docs/p0_1_status.md`。

下一步用同一运行环境实施 Spark 全量质量审计：解析行数、重复键、雨量合法性、2017 时间刻度、站点月度覆盖和元数据变化。大数据运行参数应在单年实测后调整，不能沿用本次 2 GiB / 两线程设置并假定全量一定可用。
