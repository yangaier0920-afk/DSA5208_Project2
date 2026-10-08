# P0-3 数据接口与复现

## 当前规则与范围

用户已确认全部15项决定，包括E2。正式版本为 `p0_3_v1`；P0-3只生成标准化观测，不构造未来标签，也不运行模型。完成状态必须以 `outputs/p0_3_clean/p0_3_v1/run_status.json`、`summary.json` 和 `validation.json` 为准。

原始八年CSV保留不动。处理前逐文件重新计算完整SHA-256并核对原始manifest，处理后检查大小与修改时间未变。每个年度输出重新用Spark读取，验证主键、时间网格、分区、数量和Decimal雨量账目。

## 交付目录

| 路径（相对项目根目录） | 含义 |
|---|---|
| `data/processed/p0_3_v1/observations/year=YYYY/month=M/` | 有效、主键唯一的观测；按对齐区间终点分区 |
| `data/processed/p0_3_v1/quarantine/year=YYYY/` | 不合规则或雨量冲突的原始记录；空表也写出带schema的Parquet |
| `data/processed/p0_3_v1/collision_lineage/year=YYYY/` | 碰撞组中双方记录及RETAINED/MERGED_DUPLICATE/QUARANTINED_CONFLICT处置 |
| `data/processed/p0_3_v1/station_metadata_history/year=YYYY/` | 去重之前的有效候选元数据组合、计数与最早/最晚时间；时间范围不表示组合连续生效 |
| `outputs/p0_3_clean/p0_3_v1/` | 年度账目、覆盖表、碰撞样例、schema、运行状态及交付哈希 |

## observations 关键字段

主键是 `(station_id, slot_end_ts)`，数值等价键为 `(station_id, slot_end_epoch_s)`。Spark session必须设置 `Asia/Singapore`；Parquet时间戳保存时刻，显示与日历分组依赖session时区。

| 字段 | 类型／含义 |
|---|---|
| `station_id` | 原始站点ID，保留S113；不采用最新坐标覆盖历史 |
| `event_ts` / `event_epoch_s` | 解析后的原始观测时间；不移动 |
| `slot_end_ts` / `slot_end_epoch_s` | 2017年offset=299时加一秒；标准刻度不变 |
| `alignment_offset_s` / `alignment_rule` | 0或1秒、IDENTITY或T2_PLUS_1_SECOND |
| `rain_mm` | Double，供特征与一般显示使用 |
| `rain_decimal_mm` | Decimal(38,18)，供精确累计与严格 `> h` 标签使用；源文本有效小数位超过存储能力时停止，不静默舍入 |
| `rain_period_start_ts` | `slot_end_ts - 300秒`；区间含义L1是项目假设 |
| `observation_date` | 对齐终点日历日，用于观测覆盖 |
| `rain_period_date` / `rain_period_hour` | 区间起点日历日／小时，用于D1天气分析 |
| `year` / `month` | 对齐终点的分区键；不是D1分析年月 |
| `raw_*` | 原始12列逐一保留为字符串；包括原始日期、三个时间字段、雨量、单位、类型和元数据 |
| `raw_line` / `raw_record_sha256` | 原始CSV记录及其SHA-256；哈希不是文件行号，也不区分内容完全相同的重复出现 |
| `source_file` / `source_year` / `source_sha256` | 原始文件及内容哈希，绑定原始manifest |
| `aligned_source_count` | 对齐键对应的有效候选原始记录数 |
| `metadata_conflict` | 一致雨量碰撞组中元数据不同的标志；完整差异保留在lineage与元数据历史中 |
| `metadata_warning` | 元数据／更新时间解析问题；不等同雨量失效 |
| `update_ts` / `reading_update_ts` / 相应delay字段 | 更新时间及延迟，仅供审计；不能作为预测特征 |
| `rules_version` / `quarantine_reasons` | 清洗规则版本、隔离原因数组；observations中原因数组为空 |

源码另外保留结构和精度检查字段，以便复核。实际完整字段及类型见 `outputs/p0_3_clean/p0_3_v1/schemas/`；B/C从根目录读取时，以 `root_interfaces.json` 为准，其中包含自动发现的year/month分区字段。

## 数量账目与规则

清洗账目为 `原始记录 = observations + quarantine + MERGED_DUPLICATE记录`。collision_lineage同时包含保留与移除两侧，不能把整个lineage的行数再次加到账目中。

解析结构异常、缺站点ID、无效观测时间、异常时区、原始日期／来源年矛盾、非有限或负雨量、异常单位／类型、无法按已选规则对齐的相位，进入quarantine。同键雨量不同整组隔离；数值相同则优先原标准刻度记录，之后按原始记录哈希和文本确定性排序。一致雨量不相加，也不平均。合法零值与极端值保留，不按0.2mm舍入，不作任意截断。

精度不足是程序停止条件，不能把原本合法的雨量因为存储选择而排除。源精度全量测量结果写在summary中；使用18位存储小数是统一接口容量，不是新增测量精度。

缺测不生成伪观测、不补零。station-month覆盖按全年完整日历为分母，所以年度首尾与全网缺口都计入；零覆盖月份显式记录。覆盖表中的雨量总和只是已观测量，不能用它填补缺测或直接解释为完整月总雨量。

00:00的记录在D1下归到前一天。例如2018-01-01 00:00位于2018观测分区，但雨量归到2017-12-31。B应先读取跨年观测，再按 `rain_period_start_ts` 聚合；不只读取 `year=2017` 就计算2017雨量。资料边界外的缺失不补造。

## B/C 的读取方式

在已配置好的Spark session中：

```python
from pathlib import Path
from pyspark.sql import functions as F
spark.conf.set('spark.sql.session.timeZone', 'Asia/Singapore')
base = Path('data/processed/p0_3_v1')
obs = spark.read.parquet((base / 'observations').as_posix())
quarantine = spark.read.parquet((base / 'quarantine').as_posix())
# 天气分析使用区间年月；缺测与覆盖资格另查覆盖表。
periods = obs.withColumn('rain_year', F.year('rain_period_start_ts'))
```

C构造历史／未来窗口必须检查每个预期五分钟槽，不能拿“后六行”当未来30分钟。当前P0-3数据含2024观测供质量检查与最后测试资料准备；E2的模型训练、编码拟合、特征选择及阈值范围选择必须遵循独立的时间过滤规则。

## Windows 复现命令

```powershell
# P0-2交付前置检查
.\.venv\Scripts\python.exe scripts\validate_p0_2_outputs.py
# 合成异常与Parquet检查；正式验收使用的路径见self_test.json
.\.venv\Scripts\python.exe scripts\run_spark.py src\02_clean_standardize.py --self-test --output data/processed/p0_3_self_test_v1 --evidence outputs/p0_3_clean/self_test_v1
# 八年正式清洗
.\.venv\Scripts\python.exe scripts\run_spark.py src\02_clean_standardize.py
# 从B/C使用的合并目录重读，验证分区与接口
.\.venv\Scripts\python.exe scripts\run_spark.py src\02b_verify_parquet.py
# 生成覆盖汇总、报告并校验交付
.\.venv\Scripts\python.exe scripts\validate_p0_3_outputs.py
```

已有目录不会自动覆盖；中断续跑加 `--resume`，代码、配置、来源、输出签名须相同。独立重跑应同时指定新的 `--output` 与 `--evidence`，并让交付校验程序指向对应目录。不要删除原始CSV来腾空间。
