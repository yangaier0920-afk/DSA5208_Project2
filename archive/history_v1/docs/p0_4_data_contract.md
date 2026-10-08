# P0-4：真实累计量、窗口完整性与E2用途

用户已确认按原计划继续由A实施P0-4。数据版本 `p0_4_v1` 依赖已验收的 `p0_3_v1` observations；本阶段不拟合模型、不实现B的历史雨量特征、不展开阈值样本，也不根据2024标签选择实验方案。执行及交付完成状态以 `configs/processing_status.json`、阶段run_status和validation为准。

## 定义

每条有效观测作为一个预测锚点 `(station_id,prediction_ts)`，一条观测对应一条targets记录，失效锚点保留。

- L1：以对齐后的slot_end_ts作为预测时间t。
- 未来30分钟：精确寻找t+300、600、900、1200、1500、1800秒六槽。每槽都有有效观测才保存完整累计量，否则future_30m_mm为null。有效的零累计量保存为Decimal零。
- H1历史完整性：检查t-3300至t每300秒一槽，共12槽，覆盖截至t的过去60分钟。这里仅保存完整性，不计算供模型使用的历史累计特征。
- A1：采用已声明的历史回溯事件时间可用性假设；不以最终更新时间作为预测特征。
- 阈值比较：C在训练阶段用精确Decimal累计量严格比较 `future_30m_mm > threshold_mm`；相等时label=0，无效未来窗口不能展开成负例。

Spark按station_id、实际epoch秒使用时间范围窗口，并逐个核对预期槽的成员关系。源键已由P0-3验证唯一且位于五分钟网格；P0-4额外检查有效槽数＋缺槽数分别等于6、12。不能使用源CSV行序、固定“后六行”或缺测补零。

每年读取本年及前一年末55分钟、后一年前30分钟的上下文，先计算窗口，再选择本年的锚点。这样跨月／年窗口不会被文件边界人为截断。2016/2025数据未提供，范围外上下文保持未知。

## 输出与字段

数据位于 `data/processed/p0_4_v1/targets/year=YYYY/month=M/`，质量证据位于 `outputs/p0_4_targets/p0_4_v1/`。

| 字段 | 含义 |
|---|---|
| station_id / prediction_ts / prediction_epoch_s | 唯一锚点；时间戳显示使用Asia/Singapore |
| label_end_ts / label_end_epoch_s | t+1800秒 |
| future_30m_mm | Decimal(38,18)真实未来累计量；无效时null；绝不能进入模型特征向量 |
| future_n_valid / future_missing_offsets_s | 实际未来范围内有效槽数、缺失槽相对t的秒数 |
| target_valid / invalid_reason | 未来累计量是否完整；原因数组包含MISSING_FUTURE_SLOTS，资料末端另标DATASET_END_CONTEXT_UNAVAILABLE |
| historical_n_valid / historical_missing_offsets_s | 12个历史预期槽的数量与缺失位置 |
| historical_valid / historical_invalid_reason | H1完整性；资料起点另标DATASET_START_CONTEXT_UNAVAILABLE |
| model_anchor_eligible | target_valid且historical_valid；用于训练／评价的窗口基础资格，仍须结合轮次用途和I1已知站点范围 |
| year_boundary_safe | label_end与预测锚点是否同日历年，仅供诊断；不能据此一律删掉所有跨年训练锚点 |
| fold1_role / fold2_role / fold3_role / final_role | 该锚点在各开发轮／最终阶段的用途，见下表 |
| year / month | 预测锚点的新加坡时间分区 |
| targets_version / observations_version | 输出与来源版本 |

缺测的物理原因无法由窗口表确定；MISSING_FUTURE_SLOTS只表示资料中没有完整六槽，不代表没有下雨或站点已永久停运。invalid_reason数组可能有多个原因，分原因计数不能直接相加当作互斥失效总数。

## E2用途与边界

| 字段 | TRAIN | VALIDATION或TEST |
|---|---|---|
| fold1_role | 2017—2020 | 2021 VALIDATION |
| fold2_role | 2017—2021 | 2022 VALIDATION |
| fold3_role | 2017—2022 | 2023 VALIDATION |
| final_role | 2017—2023，仅配置锁定后重训 | 2024 TEST，仅最终评估 |

未参与该轮的年份为UNUSED。若标签末端达到该段的排他右边界，记EXCLUDED_BOUNDARY，包括恰好等于边界。角色标记与窗口是否完整是两回事：跨分区的累计量可以由完整观测算出，但不能用于该分区拟合或评价。

例如2019-12-31 23:30的未来累计量可以跨到2020-01-01 00:00，仍属于fold1的同一训练段；不能只因跨日历年把它删掉。2020-12-31 23:30在fold1为EXCLUDED_BOUNDARY，在fold2/fold3则可能属于TRAIN。

本表没有提供一个固定split列，因为2021在第一轮是验证年、后两轮又是训练年。C必须按相应role读取；不能把三轮验证年整体排除出后续训练。即使最终阶段标注TRAIN，也必须先在开发轮次选择配置，再用2017—2023重训。

## C读取示例

```python
from pathlib import Path
from decimal import Decimal
from pyspark.sql import functions as F
spark.conf.set('spark.sql.session.timeZone','Asia/Singapore')
targets=spark.read.parquet(Path('data/processed/p0_4_v1/targets').as_posix())
train=targets.filter((F.col('fold1_role')=='TRAIN') & F.col('model_anchor_eligible'))
valid=targets.filter((F.col('fold1_role')=='VALIDATION') & F.col('model_anchor_eligible'))
# 仅为精确比较用法示例，不是已经冻结的阈值范围。
train=train.withColumn('threshold_mm',F.lit(Decimal('1.0')).cast('decimal(38,18)'))
train=train.withColumn('label',(F.col('future_30m_mm')>F.col('threshold_mm')).cast('double'))
```

B生成features后按 `(station_id,prediction_epoch_s)` 一对一关联。历史特征、站点编码、标准化及模型参数依然按各轮训练数据拟合；targets不是可直接作为特征的整张表。P1采样与阈值展开由C负责，不在A的全量targets中提前抽样。

实际调用预测接口时只能检查已有历史及站点／时间／阈值支持范围，不能要求未来六槽已经存在。target_valid和model_anchor_eligible用于监督训练与可评价样本筛选；未来累计量、未来缺测信息、这些含未来信息的资格字段都不能作为预测输入或事前预测门槛。历史完整但未来标签未知的锚点，可以具备预测输入条件，只是无法用当前资料核验其预测结果。

## 验收与复现

合成测试使用独立Python epoch-key/Decimal逐条复算。真实人工复算案例仅从2017—2020开发训练资料选择，导出18个预期位置和实际雨量；未来缺槽、历史缺槽、零累计、非零累计、跨日／月／年及2017相位切换有相应案例。2024只检查完整性、用途隔离和结构，不统计阈值标签分布来决定模型。

```powershell
.\.venv\Scripts\python.exe scripts\run_spark.py src\03_build_targets.py --self-test --output data/processed/p0_4_self_test_v1 --evidence outputs/p0_4_targets/self_test_v1
.\.venv\Scripts\python.exe scripts\run_spark.py src\03_build_targets.py
.\.venv\Scripts\python.exe scripts\run_spark.py src\03b_verify_targets.py
.\.venv\Scripts\python.exe scripts\validate_p0_4_outputs.py
```

已有结果不自动覆盖；中断续跑使用--resume并保持代码、配置与来源签名相同。独立重跑应同时指定新的--output与--evidence。不要在P0-4运行后用旧P0-3校验脚本覆盖阶段状态；已有P0-3交付哈希与收据由本阶段核验，输入保持不变。
