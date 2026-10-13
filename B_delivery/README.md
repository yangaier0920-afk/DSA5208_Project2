# 当前使用版本（2026-10-13）

**优先阅读**：report_zh/B_report_zh.md（唯一保留的可编辑中文报告）。两个notebooks仍在notebooks/；原数值特征版本仍是B_v2，本次补充定义、时间口径、保留率、关联核验和中文报告，不改变26列数值。

- docs/TIME_CONTRACT.md：区间与t可用观测、未来六槽、A/B/C待确认边界。
- docs/FEATURE_DEFINITIONS.md 和 feature_definitions.csv：26列完整定义。
- results/features_v2/history_retention_by_year.csv：逐年历史筛选保留率。
- results/task1_v2/coverage_sensitivity_summary.csv：90%/95%站点数与极值年份比较。
- results/join_audit/：A正式标签/B特征的真实关联结果，C已保存实际输入PASS证据，见C_delivery/results/C_v1_20261012。
- src/B_join_audit.py：可被C复用的关联核验；运行 `python B_delivery/src/B_join_audit.py --root "$PWD"`。
- results/hardware_inventory.json：后补硬件清单，不含设备标识；未测峰值内存。

原始时间戳的区间含义与t时刻观测可用性均是项目明确约定，不能写成官方证明或已经验证的真实在线部署。

以下为包说明，当前报告生成入口已切换为中文版本。

# B 交付：Task 1 + 历史特征

本包已用 2017—2024 正式八年数据运行，不是样本演示。A 的冻结代码、配置与正式数据未被修改；B 的程序和结果全部放在独立目录。

## 内容

- `src/B_task1_analysis.py`：Spark 月度、小时、网络日覆盖聚合。
- `src/B_plot_results.py`：仅读取 Spark 汇总表，生成九组 PNG/PDF 图及每图来源表。
- `src/B_build_features.py`：按年生成全量历史特征，包含跨年上下文和独立校验。
- `src/B_feature_api.py`：交给 C 的输入特征清单和单锚点预测时历史特征入口。
- `src/B_make_report.py`：依据实际结果生成完整中文 B 报告（仅Markdown）。
- `src/B_finalize.py`：刷新来源元数据及输出文件哈希。
- `configs/B_experiment.json`：B 实验配置；不修改 A 的 project.json。
- `results/task1_v2/`：统计表、图、验证证据和结果元数据。
- `results/features_v2/parquet/`：交给 C 的全量 Parquet 特征，按预测锚点年份分区。
- `results/features_v2/validation.json`：数量、缺测语义和独立时间戳核验结果。
- `report_zh/B_report_zh.md`：可复制合并进组报告的 B 中文章节，不是完整课程提交报告。
- `C_HANDOFF.md`：给 C 的读取、关联和防泄漏说明。

## 如何复跑

先完成项目 README 的正式数据下载及全量验收。下面假设本包位于项目根目录下 `B_delivery/`；路径包含空格时必须加引号。

```bash
cd /你的路径/DSA5208_Project2
source .venv/bin/activate
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
python -m pip install -r B_delivery/requirements-B.txt

python B_delivery/src/B_task1_analysis.py --root "$PWD" \
  --output B_delivery/results/task1_rerun1
python B_delivery/src/B_plot_results.py --input B_delivery/results/task1_rerun1 --root "$PWD"

python B_delivery/src/B_build_features.py --root "$PWD" \
  --output B_delivery/results/features_rerun1

python B_delivery/src/B_validate_feature_api.py --root "$PWD" \
  --features B_delivery/results/features_rerun1

python B_delivery/src/B_make_report.py \
  --task1 B_delivery/results/task1_rerun1 \
  --features B_delivery/results/features_rerun1 \
  --output B_delivery/report_rerun1

python B_delivery/src/B_finalize.py --root "$PWD" \
  --task1 B_delivery/results/task1_rerun1 \
  --features B_delivery/results/features_rerun1 \
  --report B_delivery/report_rerun1
```

复跑要使用新输出目录。Spark 固定使用 local[2]、4g JVM heap、128 个 shuffle 分区、32 MiB 文件读取分区上限，禁用 AQE 分区合并。中间数据写在本包 `runtime/spark-local/`，也可用环境变量 `B_SPARK_LOCAL_DIR` 指定其他目录。两张全量表不做内存缓存。以上属于单机多线程，不是两台机器或两个节点。

脚本使用 `--root` 读取 A 的 `scripts/data_release.py`，先验证原发布清单、哈希和 schema。不要改动 A 的读取脚本或发布清单；若已有改动，先恢复正式版本。

## 分析口径

1. Task 1 使用 Asia/Singapore 的 `rain_period_start_ts` 归属年月和小时，而不是直接使用观测分区年月。
2. 雨量单位 mm；有雨槽定义为五分钟雨量 >0。缺测仍是未知。
3. 跨年主分析使用 A 的八年同月站点交集、月覆盖率 >=90%，另做 >=95% 敏感性分析。只有 5/9/10/12 月有可用交集，其他月显示不支持。
4. 年内概览按每年每月覆盖合格站点等权统计；站点构成变化，不能把这些格子直接解释成固定站网跨年趋势。月累计为观测累计，不按覆盖率补齐。
5. 小时分析额外要求站点-月份在全部 24 个小时中分别都达到 >=90% 覆盖，以便同一站点-月份的全天曲线使用同一组站点。先站点等权，再可用年-月等权；不是全国时间槽的总体概率。
6. 空间对比自动选取严格 90% 八年同月面板站点最多的月份，同数时选较早月份；本次选中十月 21 站，不作为全年排名。
7. 无显著性检验、无因果归因、无长期气候趋势结论；各结论是这八年合格观测的描述。

## 交付和提交

给 C 的主要产物是 `results/features_v2/parquet/`、`C_HANDOFF.md`、`feature_schema.json`、实验配置和特征代码。完整特征占用较大，不要直接提交到 Git；用本地共享或组内数据存储交接。提交代码时保留配置、README、报告材料及小型汇总证据。C实际关联核验必须在其筛选/采样后、阈值展开前执行；现有A/B基础表核验不能替代。

B不替C重跑模型；合并后的仓库已有C模型、评价及重载材料。全组英文材料位于submission。该目录保留队友声明的2026-10-12证据，本次没有重新生成最终PDF/ZIP；若要把新增B图表纳入最终提交，需要另行更新整合生成器及包清单。

## AI 使用

OpenAI Codex 协助设计和实现 B 的 Spark 聚合、历史特征、可视化、验证及报告草稿。数量和结论来自本机实际执行结果；程序验证不等于所有代码均已由人逐行审阅。组员仍需阅读报告、理解指标、检查所提交材料，并据实际使用补充全组 AI 声明。

## Notebook 版

`notebooks/B_Task1_Analysis.ipynb` 展开 Task 1 的 Spark 聚合与七组绘图代码；`notebooks/B_Historical_Features.ipynb` 展开因果历史特征函数、逐年写出和独立验证。默认开关全部关闭，可先查看已有结果，不会启动全量计算或覆盖原文件。Notebook 格式、全部代码单元格语法及默认查看流程已验证；已有全量结果来自通过验证的 v2 脚本运行；Notebook 默认查看流程另行实跑验证。

在项目根目录激活 `.venv`，安装 Notebook 后启动：

```bash
python -m pip install notebook
python -m notebook
```

在网页进入 `B_delivery/notebooks/` 打开对应文件。修改后按从上到下顺序运行。绘图需要在当前内核安装 matplotlib（Notebook 内有可取消注释的 `%pip` 单元格）。全部新实验使用时间戳目录；开启全量计算前应确认 Python 3.11 / Java 17 环境。特征变换修改后需同步 C 的特征清单和预测入口，重新验证离线与在线一致性。

## v2 更新（2026-10-11）

- 两个 Notebook 的标题带 v2；代码主体与 src 同步。修复了同一行两个语句在拆分单元格时被重复复制的问题。
- 新增年度描述图：全年覆盖率 >=90%/95%，并展示所有月份均 >=90% 的更严格门槛。2018 无合格站点，保留 NA；八年固定年度站点交集为空。年度均值描述实际观测和变化的站点组成，不估算缺测降雨。
- 新增频率/强度图：有雨槽比例与非零雨量 P50/P90/P99。分位数是站点-月份分位数的等权平均，不是全国汇集分位数。
- 数值输入由 14 增至 26：新增精确 lag10/15/30、湿/干状态、连续湿槽及截断标记、距近期降雨时间及截断编码、change5、ratio15/30。新增特征是否有效仍须 C 在验证年评价。
- 过去 60 分钟均湿时，连续湿槽记为 12 且标记左截断；过去 60 分钟无雨时，原始距上次降雨时间为 null，模型字段用 60 编码并带 no_rain_observed_60m=1，不能将 60 解释为真实间隔。
- 固定种子 20261011，从至少有 20 条观测的站点中抽 20 站，每站抽 20 锚点，另保留固定跨年边界检查。六个合成边界案例也通过；不缓存全量表。
- 新结果在 results/task1_v2、results/features_v2 和 report_zh；旧版本及备份已经删除。Task1 重用此前已验证的 Spark 汇总表，仅重新生成新增分析和图；历史特征全量重算。
- src/B_test_features.py 可单独运行小数据边界测试。完整特征不要推到 GitHub，使用组内数据存储交接。

## 当前报告与文件

report_zh/只保留B_report_zh.md。图文件保留在results/task1_v2/figures/，报告通过相对路径引用。报告来源及哈希记录在results/report_metadata/。

旧版本、归档备份和Notebook旧检查点已经删除。当前代码在src/，Notebook在notebooks/，当前结果使用task1_v2、features_v2及join_audit。目录名中的v2是当前特征版本，不是旧版副本。

可复跑关联核验的边界案例：`python B_delivery/src/B_test_join_audit.py --root "$PWD"`。

## Task 1完善（2026-10-13）

- 覆盖敏感性差值定义为同一年两门槛面板均值之差的最大绝对值，报告含公式。
- 新增`src/B_station_map.py`，核对A元数据历史，图8严格复用图5的同一站点面板。
- 站点位置图使用最新记录坐标；多坐标并不证明搬迁，固定编号不保证测量条件均一。
- 中文报告和Task1 Notebook补充MSS官方天气背景引用，不作因果归因。
- 单独复跑地图：`python B_delivery/src/B_station_map.py --root "$PWD"`；完整绘图命令增加`--root "$PWD"`以同时生成地图。
- 本次不重算Spark聚合、历史特征或C模型，不沿用旧耗时描述新增地图耗时。

### 简单局部跨年补充

新增`src/B_adjacent_years.py`，比较七对相邻年份、12个月，共84个格子。每格只使用两年同月覆盖率均≥90%的共同站点；后一年减前一年，等权，缺共同站点为NA。面板随格子变化，不能串成全国长期趋势或全年累计。

单独运行：`python B_delivery/src/B_adjacent_years.py`。完整绘图脚本也会生成图9。来源表为`results/task1_v2/adjacent_year_same_month_summary.csv`，包含每格完整站点清单；核验为`adjacent_year_validation.json`。图、中文报告及Task1 Notebook同步，默认查看不重算。

### 当前图表阅读顺序与样式

报告和Notebook统一为：图1覆盖 → 图2月度概览 → 图3频率/强度 → 图4小时 → 图5年度概览 → 图6严格八年同月 → 图7相邻年份局部比较 → 图8站点地图 → 图9同面板站点柱状图。已有图片文件名保留以保持来源可追溯，文件名前缀不等于当前阅读图号。地图改为单面板青绿渐变，坐标变化仅在图注说明；相邻年份图采用橙色（减少）—青绿（增加）配色。

## 合并后的材料入口

- 当前九图中文B稿：report_zh/B_report_zh.md；可编辑Notebook保持原路径。
- 队友英文B参考稿：report_en/B_report_en.md，保留2026-10-12证据范围。对应生成器为src/B_make_report_en.py，独立输出，不覆盖中文稿。
- 英文时间/特征定义保留原文件名，中文生成器使用docs/TIME_CONTRACT_zh.md和FEATURE_DEFINITIONS_zh.md。
- C保存的实际输入关联和模型核验见C_delivery；submission为队友固定实验版本，不自动包含本次新增B图。
- results/*_rerun_20261012和report_rerun_20261012属于模型/提交材料引用的实验来源，本次合并保留，避免破坏证据链。
