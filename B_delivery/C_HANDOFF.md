# C 交接说明

## 读取与一对一关联

```python
from pathlib import Path
import sys
from pyspark.sql import functions as F

project_root = Path('/你的路径/DSA5208_Project2')
b_root = project_root / 'B_delivery'
sys.path.insert(0, str(project_root / 'scripts'))
sys.path.insert(0, str(b_root / 'src'))
from data_release import read_tables
from B_feature_api import NUMERIC_FEATURES
from B_join_audit import audit_anchor_join

spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
targets = read_tables(spark, project_root, scope='full')['targets']
features = spark.read.parquet(str(b_root / 'results/features_v2/parquet'))
# 不把 features.year、时间戳、完整性字段或整个 targets 表直接喂入模型。
feature_inputs = features.select('station_id', 'prediction_epoch_s', *NUMERIC_FEATURES)

role = 'fold1_role'  # 后续轮次换 fold2_role / fold3_role / final_role
supervised = targets.filter(
    F.col(role).isin('TRAIN', 'VALIDATION', 'TEST') & F.col('model_anchor_eligible')
)
audit = audit_anchor_join(supervised, feature_inputs, scope='C_actual_pre_threshold_expansion')
assert audit['status'] == 'PASS', audit
# 将audit保存为本轮实际输入证据，再执行关联；不要先去重。
joined = supervised.join(feature_inputs, ['station_id', 'prediction_epoch_s'], 'inner')
assert joined.count() == audit['inner_join_rows'] == audit['left_rows']
```

`station_id + prediction_epoch_s` 是唯一主键。观测表对应 `station_id + slot_end_epoch_s`，不要只按日期或站点关联。特征文件保留每个观测锚点，未进行训练采样或阈值展开。

## 输入清单

`NUMERIC_FEATURES` 定义在 `src/B_feature_api.py`，包括当前雨量、前一个五分钟雨量、15/30/60 分钟累计、前一段 15 分钟累计、近期变化、60 分钟最大值/总体标准差/有雨槽数和小时/月周期编码。station_id 由 C 在各轮训练数据上拟合编码。

历史审计字段 n_valid_* 和 history_complete_60m 均只依赖过去。它们主要用于验证输入窗口，默认不加入模型输入；完整历史模型在推理时必须检查 history_complete_60m，而不是检查未来标签是否存在。

每个窗口使用精确 epoch 秒范围，当前 t 槽包含在内。最近 60 分钟有 12 个区间末端 t-3300,...,t，其实际雨量区间覆盖 (t-3600,t]。窗口不完整时该窗口汇总为 null，绝不补零。特征采用 double，监督标签严格阈值比较继续使用 A 的 Decimal 累计。

## C 仍需完成

- 按 E2 做 2017-2020 -> 2021、2017-2021 -> 2022、2017-2022 -> 2023 验证；锁定配置后 2017-2023 重训，2024 仅最终测试。
- role 和 model_anchor_eligible 仅筛选监督样本，不能作为特征或事前预测门槛。
- 阈值候选 0/0.5/1/2/5 mm 的支持范围只用最早训练段确定；未知 future_30m_mm 不能变成负例。
- 如果按组内方案采样，先采锚点再展开阈值，固定 seed；B 的输出是未采样全量。
- 每轮 station 编码、标准化、插补等只在训练数据拟合，不能先在八年全量数据拟合。
- 标签使用严格 >，不是 >=。用 Spark MLlib 输出概率和评价；保存和重载模型，并验证预测入口。
- 最终评价需区分阈值和验证年份，遵循组内 Brier 等权选择协议。准确率不能单独说明稀有降雨事件的预测质量。

## 推理时复用特征

```python
from B_feature_api import features_at
one_anchor = features_at(observations, 'S24', prediction_epoch_s)
```

此入口只读取该站点过去 60 分钟，要求当前锚点及全部 12 个历史槽存在。它不读取标签或未来数据。C 还须检查训练中支持的站点、阈值，并使用已拟合模型的相同编码流程。

## 关于 2024 数据

B 的 Task 1 按课程要求描述全部 2017-2024，包括 2024。特征是预先确定、无需学习的因果变换，也生成到 2024。C 仍不能用 2024 标签、模型误差或表现来选择特征、参数、阈值或模型；2024 保留作一次最终评价。

## B_v2 新输入与语义

以 B_feature_api.NUMERIC_FEATURES 的 26 个字段为准，不要硬编码旧 14 列。新增 lag_10m_mm/lag_15m_mm/lag_30m_mm、wet_current/wet_lag_5m/wet_lag_10m、consecutive_wet_slots_60m、wet_run_left_censored_60m、minutes_since_last_rain_60m_capped、no_rain_observed_60m、rain_change_5m_mm、rain_ratio_15m_30m。

连续湿槽最多观察 12 槽；全部湿时左截断标记为 1。原始 minutes_since_last_rain_60m 未观察到近期雨时为 null，默认模型不使用它；模型使用 capped 字段的 60 截断编码和 no_rain_observed_60m 标记。60 不能解释为准确间隔。ratio 在完整 30 分钟且分母为零时为 0；缺测仍为 null。

station_id 保留作类别输入，C 必须在每轮训练集内拟合 StringIndexer/OneHotEncoder，并处理未知站点。新增特征仅为候选，是否采用须验证；2024 不参与选择。预测时和离线生成统一复用更新后的 make_features。

## 时间约定、完整定义与关联证据（本次更新）

请共同核对 docs/TIME_CONTRACT.md 与 docs/FEATURE_DEFINITIONS.md。对齐后t为区间终点，R(t)对应(t−5min,t]；事件时间回溯假设下可用。未来标签只有t+5至t+30的六槽，排除R(t)。官方语义与真实发布延迟未独立证实，不能把假设当官方事实。

A/B基础表已实跑关联，证据位于results/join_audit；不代表C实际筛选/采样后的输入已验收。上方函数要求两侧主键无空值且唯一，并核对匹配率、未匹配、关联前后行数与膨胀量。左侧监督子集要求全部匹配，右侧全量未使用行允许存在。请保存每轮实际scope证据。阈值展开后改查三列键（站点、时间、阈值），以及展开前后行数/每锚点阈值数。

完整历史保留率见results/features_v2/history_retention_by_year.csv，B仍保留全部锚点，完整历史模型排除缺历史锚点；model_anchor_eligible同时含未来与历史资格，最终训练率不能由历史率直接推断。
