# 项目补充资料（查阅用）

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

所有timestamp按Asia/Singapore显示，epoch为Unix秒。Parquet的nullable是存储schema属性，并不表示每个字段实际允许缺失；观测有效性及标签空值约束见本文件的数据和标签规则。原始与审计字段保留用于追溯，不能整表直接传入模型。

## observations

| 字段 | Spark类型 | schema nullable | 定义与用途 |
|---|---|---|---|
| raw_line | string | true | 原始CSV整行，原样追溯；不作为模型输入 |
| field_count | integer | true | 原始CSV字段数量，应为12 |
| odd_quotes | boolean | true | 原始整行引号数是否为奇数，解析审计标记 |
| raw_date | string | true | 原始日期，未经替换，供追溯 |
| raw_timestamp | string | true | 原始观测时间文本，未经替换，供追溯 |
| raw_update_timestamp | string | true | 原始更新时间文本，未经替换，供追溯 |
| raw_station_id | string | true | 原始站点ID，未经替换，供追溯 |
| raw_station_name | string | true | 原始站名，未经替换，供追溯 |
| raw_station_device_id | string | true | 原始设备ID，未经替换，供追溯 |
| raw_location_longitude | string | true | 原始经度文本，未经替换，供追溯 |
| raw_location_latitude | string | true | 原始纬度文本，未经替换，供追溯 |
| raw_reading_update_timestamp | string | true | 原始读数更新时间文本，未经替换，供追溯 |
| raw_reading_value | string | true | 原始雨量文本，源单位mm，未经替换，供追溯 |
| raw_reading_type | string | true | 原始读数类型文本，未经替换，供追溯 |
| raw_reading_unit | string | true | 原始单位文本，期望mm，未经替换，供追溯 |
| _corrupt_record | string | true | Spark捕获的解析失败原文；有效观测应为空 |
| source_year | integer | true | 原始年度文件年份 |
| source_file | string | true | 原始文件名／路径标识 |
| source_sha256 | string | true | 原始文件全文SHA-256 |
| raw_record_sha256 | string | true | 原始整行SHA-256，追溯键；不替代观测主键 |
| station_id | string | true | 保留原始站点ID（含S113）；用于主键及按站点分组 |
| station_name | string | true | 记录对应的站点名称，允许历史变化 |
| station_device_id | string | true | 记录对应的设备ID |
| event_ts | timestamp | true | 解析的原始观测时间，尚未进行2017对齐 |
| update_ts | timestamp | true | 解析的原始update_timestamp；审计用，不作为天气特征 |
| reading_update_ts | timestamp | true | 解析的原始reading_update_timestamp；审计用，不作为天气特征 |
| rain_mm | double | true | 该五分钟区间雨量，double，单位mm；特征计算可用，精确阈值比较用Decimal |
| rain_decimal_mm | decimal(38,18) | true | 该五分钟区间精确雨量，Decimal(38,18)，mm；标签累计使用 |
| longitude | double | true | 原行经度，度；不回填未来最新位置 |
| latitude | double | true | 原行纬度，度；不回填未来最新位置 |
| event_epoch_s | long | true | 原始观测时间的Unix秒 |
| original_offset_s | long | true | 原始epoch对300取余，秒 |
| alignment_offset_s | integer | true | 实际对齐增加秒数（0或1） |
| alignment_rule | string | true | 实际采用的对齐规则标识 |
| slot_end_epoch_s | long | true | 对齐后的五分钟雨量区间末端Unix秒，观测主键时间 |
| slot_end_ts | timestamp | true | 对齐后的区间末端／预测锚点t，五分钟网格 |
| rain_period_start_ts | timestamp | true | 区间起点slot_end_ts-300秒，Task 1日历归属依据 |
| observation_date | date | true | 末端时间所属新加坡日历日期 |
| rain_period_date | date | true | 区间起点所属新加坡日历日期 |
| rain_period_hour | integer | true | 区间起点所属新加坡小时，0至23 |
| update_delay_s | long | true | 更新时间减原始观测时间，秒；仅审计 |
| reading_update_delay_s | long | true | 读数更新时间减原始观测时间，秒；仅审计 |
| quarantine_reasons | array<string> | true | 隔离原因数组；有效observations为空数组 |
| metadata_warning | boolean | true | 名称／设备缺失、坐标解析／范围异常或更新时间解析缺失的警示；保留有效雨量 |
| source_effective_decimal_scale | integer | true | 原始数值有效小数位数，精确表示审计 |
| decimal_representation_error | boolean | true | 无法按既定Decimal精确表示的标记；有效观测为false |
| rules_version | string | true | 清洗规则版本标识 |
| aligned_source_count | long | true | 映射到相同标准键的源行数量 |
| metadata_conflict | boolean | true | 对齐碰撞中元数据存在差异；雨量相等仍可保留 |
| year | integer | true | 对齐末端／预测锚点所属新加坡年份，分区键 |
| month | integer | true | 对齐末端／预测锚点所属新加坡月份，分区键1至12 |

## quarantine

| 字段 | Spark类型 | schema nullable | 定义与用途 |
|---|---|---|---|
| raw_line | string | true | 原始CSV整行，原样追溯；不作为模型输入 |
| field_count | integer | true | 原始CSV字段数量，应为12 |
| odd_quotes | boolean | true | 原始整行引号数是否为奇数，解析审计标记 |
| raw_date | string | true | 原始日期，未经替换，供追溯 |
| raw_timestamp | string | true | 原始观测时间文本，未经替换，供追溯 |
| raw_update_timestamp | string | true | 原始更新时间文本，未经替换，供追溯 |
| raw_station_id | string | true | 原始站点ID，未经替换，供追溯 |
| raw_station_name | string | true | 原始站名，未经替换，供追溯 |
| raw_station_device_id | string | true | 原始设备ID，未经替换，供追溯 |
| raw_location_longitude | string | true | 原始经度文本，未经替换，供追溯 |
| raw_location_latitude | string | true | 原始纬度文本，未经替换，供追溯 |
| raw_reading_update_timestamp | string | true | 原始读数更新时间文本，未经替换，供追溯 |
| raw_reading_value | string | true | 原始雨量文本，源单位mm，未经替换，供追溯 |
| raw_reading_type | string | true | 原始读数类型文本，未经替换，供追溯 |
| raw_reading_unit | string | true | 原始单位文本，期望mm，未经替换，供追溯 |
| _corrupt_record | string | true | Spark捕获的解析失败原文；有效观测应为空 |
| source_year | integer | true | 原始年度文件年份 |
| source_file | string | true | 原始文件名／路径标识 |
| source_sha256 | string | true | 原始文件全文SHA-256 |
| raw_record_sha256 | string | true | 原始整行SHA-256，追溯键；不替代观测主键 |
| station_id | string | true | 保留原始站点ID（含S113）；用于主键及按站点分组 |
| station_name | string | true | 记录对应的站点名称，允许历史变化 |
| station_device_id | string | true | 记录对应的设备ID |
| event_ts | timestamp | true | 解析的原始观测时间，尚未进行2017对齐 |
| update_ts | timestamp | true | 解析的原始update_timestamp；审计用，不作为天气特征 |
| reading_update_ts | timestamp | true | 解析的原始reading_update_timestamp；审计用，不作为天气特征 |
| rain_mm | double | true | 该五分钟区间雨量，double，单位mm；特征计算可用，精确阈值比较用Decimal |
| rain_decimal_mm | decimal(38,18) | true | 该五分钟区间精确雨量，Decimal(38,18)，mm；标签累计使用 |
| longitude | double | true | 原行经度，度；不回填未来最新位置 |
| latitude | double | true | 原行纬度，度；不回填未来最新位置 |
| event_epoch_s | long | true | 原始观测时间的Unix秒 |
| original_offset_s | long | true | 原始epoch对300取余，秒 |
| alignment_offset_s | integer | true | 实际对齐增加秒数（0或1） |
| alignment_rule | string | true | 实际采用的对齐规则标识 |
| slot_end_epoch_s | long | true | 对齐后的五分钟雨量区间末端Unix秒，观测主键时间 |
| slot_end_ts | timestamp | true | 对齐后的区间末端／预测锚点t，五分钟网格 |
| rain_period_start_ts | timestamp | true | 区间起点slot_end_ts-300秒，Task 1日历归属依据 |
| observation_date | date | true | 末端时间所属新加坡日历日期 |
| rain_period_date | date | true | 区间起点所属新加坡日历日期 |
| rain_period_hour | integer | true | 区间起点所属新加坡小时，0至23 |
| update_delay_s | long | true | 更新时间减原始观测时间，秒；仅审计 |
| reading_update_delay_s | long | true | 读数更新时间减原始观测时间，秒；仅审计 |
| quarantine_reasons | array<string> | true | 隔离原因数组；有效observations为空数组 |
| metadata_warning | boolean | true | 名称／设备缺失、坐标解析／范围异常或更新时间解析缺失的警示；保留有效雨量 |
| source_effective_decimal_scale | integer | true | 原始数值有效小数位数，精确表示审计 |
| decimal_representation_error | boolean | true | 无法按既定Decimal精确表示的标记；有效观测为false |
| rules_version | string | true | 清洗规则版本标识 |
| aligned_source_count | long | true | 映射到相同标准键的源行数量 |
| metadata_conflict | boolean | true | 对齐碰撞中元数据存在差异；雨量相等仍可保留 |
| year | integer | true | 来源原始文件年份，分区键 |

## collision_lineage

| 字段 | Spark类型 | schema nullable | 定义与用途 |
|---|---|---|---|
| station_id | string | true | 保留原始站点ID（含S113）；用于主键及按站点分组 |
| slot_end_epoch_s | long | true | 对齐后的五分钟雨量区间末端Unix秒，观测主键时间 |
| raw_line | string | true | 原始CSV整行，原样追溯；不作为模型输入 |
| field_count | integer | true | 原始CSV字段数量，应为12 |
| odd_quotes | boolean | true | 原始整行引号数是否为奇数，解析审计标记 |
| raw_date | string | true | 原始日期，未经替换，供追溯 |
| raw_timestamp | string | true | 原始观测时间文本，未经替换，供追溯 |
| raw_update_timestamp | string | true | 原始更新时间文本，未经替换，供追溯 |
| raw_station_id | string | true | 原始站点ID，未经替换，供追溯 |
| raw_station_name | string | true | 原始站名，未经替换，供追溯 |
| raw_station_device_id | string | true | 原始设备ID，未经替换，供追溯 |
| raw_location_longitude | string | true | 原始经度文本，未经替换，供追溯 |
| raw_location_latitude | string | true | 原始纬度文本，未经替换，供追溯 |
| raw_reading_update_timestamp | string | true | 原始读数更新时间文本，未经替换，供追溯 |
| raw_reading_value | string | true | 原始雨量文本，源单位mm，未经替换，供追溯 |
| raw_reading_type | string | true | 原始读数类型文本，未经替换，供追溯 |
| raw_reading_unit | string | true | 原始单位文本，期望mm，未经替换，供追溯 |
| _corrupt_record | string | true | Spark捕获的解析失败原文；有效观测应为空 |
| source_year | integer | true | 原始年度文件年份 |
| source_file | string | true | 原始文件名／路径标识 |
| source_sha256 | string | true | 原始文件全文SHA-256 |
| raw_record_sha256 | string | true | 原始整行SHA-256，追溯键；不替代观测主键 |
| station_name | string | true | 记录对应的站点名称，允许历史变化 |
| station_device_id | string | true | 记录对应的设备ID |
| event_ts | timestamp | true | 解析的原始观测时间，尚未进行2017对齐 |
| update_ts | timestamp | true | 解析的原始update_timestamp；审计用，不作为天气特征 |
| reading_update_ts | timestamp | true | 解析的原始reading_update_timestamp；审计用，不作为天气特征 |
| rain_mm | double | true | 该五分钟区间雨量，double，单位mm；特征计算可用，精确阈值比较用Decimal |
| rain_decimal_mm | decimal(38,18) | true | 该五分钟区间精确雨量，Decimal(38,18)，mm；标签累计使用 |
| longitude | double | true | 原行经度，度；不回填未来最新位置 |
| latitude | double | true | 原行纬度，度；不回填未来最新位置 |
| event_epoch_s | long | true | 原始观测时间的Unix秒 |
| original_offset_s | long | true | 原始epoch对300取余，秒 |
| alignment_offset_s | integer | true | 实际对齐增加秒数（0或1） |
| alignment_rule | string | true | 实际采用的对齐规则标识 |
| slot_end_ts | timestamp | true | 对齐后的区间末端／预测锚点t，五分钟网格 |
| rain_period_start_ts | timestamp | true | 区间起点slot_end_ts-300秒，Task 1日历归属依据 |
| observation_date | date | true | 末端时间所属新加坡日历日期 |
| rain_period_date | date | true | 区间起点所属新加坡日历日期 |
| rain_period_hour | integer | true | 区间起点所属新加坡小时，0至23 |
| update_delay_s | long | true | 更新时间减原始观测时间，秒；仅审计 |
| reading_update_delay_s | long | true | 读数更新时间减原始观测时间，秒；仅审计 |
| quarantine_reasons | array<string> | true | 隔离原因数组；有效observations为空数组 |
| metadata_warning | boolean | true | 名称／设备缺失、坐标解析／范围异常或更新时间解析缺失的警示；保留有效雨量 |
| source_effective_decimal_scale | integer | true | 原始数值有效小数位数，精确表示审计 |
| decimal_representation_error | boolean | true | 无法按既定Decimal精确表示的标记；有效观测为false |
| rules_version | string | true | 清洗规则版本标识 |
| aligned_source_count | long | true | 映射到相同标准键的源行数量 |
| rain_variants | long | true | 碰撞组中不同精确雨量的数量 |
| metadata_variants | long | true | 碰撞组中不同元数据组合数量 |
| collision_rank | integer | true | 确定性碰撞排序序号，1为优先行 |
| retained_record_sha256 | string | true | 碰撞组最终保留原行的SHA-256 |
| disposition | string | true | 碰撞原行处置，如RETAINED／MERGED_DUPLICATE |
| year | integer | true | 来源原始文件年份，分区键 |

## station_metadata_history

| 字段 | Spark类型 | schema nullable | 定义与用途 |
|---|---|---|---|
| station_id | string | true | 保留原始站点ID（含S113）；用于主键及按站点分组 |
| station_name | string | true | 记录对应的站点名称，允许历史变化 |
| station_device_id | string | true | 记录对应的设备ID |
| raw_location_longitude | string | true | 原始经度文本，未经替换，供追溯 |
| raw_location_latitude | string | true | 原始纬度文本，未经替换，供追溯 |
| raw_rows | long | true | 该站点／元数据组合在此原始年份内的源行数量（含合并前行） |
| first_event_ts | timestamp | true | 该站点／元数据组合首次出现的原始时间；不是连续有效期起点 |
| last_event_ts | timestamp | true | 该组合最后出现的原始时间；不是连续有效期终点 |
| source_file | string | true | 原始文件名／路径标识 |
| year | integer | true | 来源原始文件年份，分区键 |

## targets

| 字段 | Spark类型 | schema nullable | 定义与用途 |
|---|---|---|---|
| station_id | string | true | 保留原始站点ID（含S113）；用于主键及按站点分组 |
| prediction_epoch_s | long | true | 预测锚点t的Unix秒，与slot_end_epoch_s相同 |
| historical_n_valid | long | true | 截至t的12个历史槽实际有效数，0至12 |
| prediction_ts | timestamp | true | 预测锚点t，显示使用Asia/Singapore |
| label_end_epoch_s | long | true | t+1800秒；监督标签窗口右端 |
| label_end_ts | timestamp | true | 未来30分钟标签窗口右端时间 |
| future_missing_offsets_s | array<integer> | true | 未来六个预期槽中缺失槽的相对t偏移秒数；仅标签审计 |
| historical_missing_offsets_s | array<integer> | true | 历史12个预期槽中缺失槽的相对t偏移秒数 |
| future_n_valid | long | true | 未来六槽实际有效观测数，0至6；仅标签审计 |
| target_valid | boolean | true | 未来六槽是否齐全；监督标签筛选，禁止用作预测输入 |
| historical_valid | boolean | true | H1历史12槽是否齐全，可检查事前输入条件 |
| future_30m_mm | decimal(38,18) | true | 未来六槽精确累计mm；仅有效时有值，否则null；禁止进入特征 |
| model_anchor_eligible | boolean | true | target_valid且historical_valid；仅训练／评价基础资格，不是事前预测门槛 |
| year_boundary_safe | boolean | true | 标签末端与t是否同日历年，仅诊断；禁止据此全局删跨年锚点 |
| invalid_reason | array<string> | true | 未来标签失效原因数组，可能多原因；未知不等于无雨 |
| historical_invalid_reason | array<string> | true | 历史输入完整性失效原因数组 |
| fold1_role | string | true | E2第一轮TRAIN17—20、VALIDATION21、UNUSED／EXCLUDED_BOUNDARY |
| fold2_role | string | true | E2第二轮TRAIN17—21、VALIDATION22、UNUSED／EXCLUDED_BOUNDARY |
| fold3_role | string | true | E2第三轮TRAIN17—22、VALIDATION23、UNUSED／EXCLUDED_BOUNDARY |
| final_role | string | true | 配置锁定后TRAIN17—23、TEST24或EXCLUDED_BOUNDARY |
| targets_version | string | true | 标签表版本p0_4_v1 |
| observations_version | string | true | 来源观测版本p0_3_v1 |
| year | integer | true | 对齐末端／预测锚点所属新加坡年份，分区键 |
| month | integer | true | 对齐末端／预测锚点所属新加坡月份，分区键1至12 |

</details>

## A中文报告材料

### 数据来源与正式规模

使用data.gov.sg的2017—2024八个年度降雨CSV，五分钟间隔、源单位mm。八个源文件共8,047,044,701字节。官方数据页与本地全文SHA-256见manifests/raw_manifest.json；用户确认文件为官方导出且完整获取，这不等于已核对服务器端内容哈希。正式规模以Spark解析数48,538,713为准。

原始数据中结构、数值、单位／类型异常及原始键重复均未检出；存在2017非标准秒相位、全网63个整日资料缺口、站点覆盖与元数据变化。2018年8月4—31日没有记录，不能把缺口视作零降雨，也不能仅由文件推断物理原因。

### 清洗与时间标准化

保留原始12列文本、整行、源文件与记录哈希，以及原始event_ts。仅2017时间秒余数为299的记录增加1秒形成五分钟区间末端slot_end_ts。1,775,327条源记录触发对齐，保留表中1,775,241条仍带此对齐标记。

对齐产生86组同键且雨量一致的碰撞：优先保留原来就在标准网格上的记录，不累加也不平均；合并移除86条，双方172条记录保留在collision_lineage。两条保留观测存在元数据差异标记，历史元数据不以最新坐标覆盖。雨量不同的同键组按规则整组隔离，但本次真实数据未出现。

只保留有限且非负雨量，保留真实零与有效极端值，不进行任意截断或0.2mm舍入。标签累计使用Decimal(38,18)，源有效小数精度最高5位；无法精确表示时停止而不是静默舍入。存储18位小数不代表测量精度增加。雨量定义为截至slot_end_ts的五分钟区间量，Task 1日历归属按rain_period_start_ts=slot_end_ts-300秒。

数量账目：48,538,713 = 48,538,627正式观测 + 86合并移除 + 0隔离。合并比例为0.00017718%。隔离为空不代表时间序列完整或资料没有质量风险。正式观测中47,121,647条雨量为零；全部保留，不对未知位置创建零记录。

### 未来累计量与历史完整性

每条观测对应一个预测锚点t。按站点和实际epoch检查t+300至t+1800的六个精确位置，全部有效才保存future_30m_mm；未知累计为null，已知无雨为Decimal零。历史完整性检查t-3300至t的12个位置。先读跨年度必要上下文，再选择本年度锚点，不采用“后六行”窗口。

共有48,538,627个锚点，47,636,833个未来完整、901,794个未来未知；47,347,833个历史完整、1,190,794个历史不完整。历史与未来同时完整的基础资格为47,048,872，仍需C按E2边界与已知站点支持范围筛选。历史失效和未来失效有重叠，不能直接相加。所有失效锚点保留在targets中，不算清洗删除。

| 年份 | 原始行数 | 保留观测 | 合并移除 | 未来完整 | 未来未知 | 历史和未来均完整 |
|---|---|---|---|---|---|---|
| 2017 | 5,256,106 | 5,256,020 | 86 | 5,057,559 | 198,461 | 4,822,618 |
| 2018 | 4,594,246 | 4,594,246 | 0 | 4,079,741 | 514,505 | 3,981,287 |
| 2019 | 5,026,862 | 5,026,862 | 0 | 4,991,346 | 35,516 | 4,935,783 |
| 2020 | 6,334,473 | 6,334,473 | 0 | 6,292,286 | 42,187 | 6,234,155 |
| 2021 | 7,044,646 | 7,044,646 | 0 | 7,013,455 | 31,191 | 6,976,422 |
| 2022 | 7,133,230 | 7,133,230 | 0 | 7,108,318 | 24,912 | 7,073,178 |
| 2023 | 6,742,890 | 6,742,890 | 0 | 6,719,652 | 23,238 | 6,687,003 |
| 2024 | 6,406,260 | 6,406,260 | 0 | 6,374,476 | 31,784 | 6,338,426 |

课程标签为未来累计量严格大于给定阈值；相等不超阈，无效累计不能展开成负例。A没有进行阈值样本展开、特征学习或训练。E2角色用于2017—2020/2021、2017—2021/2022、2017—2022/2023三轮开发；锁定配置后2017—2023重训并对2024最终测试。目标或窗口信息不可进入模型特征或事前预测门槛。

### 计算环境、耗时与资源口径

本机Microsoft Windows 11 家庭版 中文版，12th Gen Intel(R) Core(TM) i7-12700H，14物理核心／20逻辑处理器，当前系统可见物理内存约31.73GiB。Python 3.11.5、Spark/PySpark 3.5.7、Py4J 0.10.9.7、Java 17.0.20.1；时区Asia/Singapore。全量运行local[4]、driver memory 4g、shuffle partitions 48；这是本地线程和JVM堆配置，不是独立四节点集群或进程总内存峰值。

| 全量阶段 | 起止收据耗时（分钟） | 年度计时合计（分钟） |
|---|---|---|
| P0-2 | 31.48 | 31.25 |
| P0-3 | 34.27 | 33.94 |
| P0-4 | 9.82 | 9.66 |

耗时从各阶段原始run_status起止时间计算，包含相应阶段初始化、处理、内置核验与退出；不包含安装、另行执行的交付校验和阶段之间的人工工作。不同阶段的年度计时口径不同，不能据此作算法性能比较。全量峰值RSS、CPU利用率与临时盘峰值当时未采集，不写成4GB或零；当前正式Parquet合计4,678,105,303字节，也不是全流程最大磁盘需求。

本机Windows本地组件为固定提交的社区Hadoop 3.3.5，Spark自带Hadoop Java为3.3.4；本工作负载实际读写通过，组件来源与局限见archive/history_v1/docs/environment.md。硬件清单是在P1-6阶段读取，不是历史资源峰值测量。

### 复现验证与可复现范围

全量阶段已有年度检查、根表读回、计数闭合和正式文件哈希验收。P0-4另有30项合成测试、29个真实人工案例及522个预期槽复算。P1-5验证八年配对样本接口读写和同机新目录迁移。

P1-6新增真实原始片段重跑：1,297条源记录，来自2017—2021必要上下文，覆盖29个开发期人工案例及全部86组碰撞，114个选定锚点。原始整行保留自已校验的observations/lineage，片段只重建表头和换行；不是重新下载的完整CSV。重用与正式版本相同且经哈希核对的解析、碰撞和窗口函数，写后重读，逐列与全量参照输出双向比较，并独立使用Python Decimal复算全部选定锚点。首次本地重跑约72.58秒，local[2]、2g、8个shuffle分区；这是功能复现而非全量性能基准。

另以单独支持包进行解压迁移重跑，验收记录见outputs/p1_6_support/validation.json。A的本机测试不代表B/C或macOS已验证；最终模型尚未由C提供，模型加载与预测一致性仍是团队后续验收。完整复現需要另获八年CSV／正式大表并核对清单；小包只支持代表性数据流程。

### 局限与AI使用声明材料

缺测不填补，因此观测累计不是缺测月份的完整真实总雨量；Task 1采用覆盖面板且须报告可比较范围。2017+1秒与区间末端解释属于已确认的工程口径，应在报告中明确。元数据历史的first/last不是连续有效期，不能据此自动推断站点移动时间。本组采用回溯事件时间可用假设，较大的更新时间延迟意味着不能把历史结果直接宣称为实时在线部署效果。

站点异质性、极端值和潜在测量误差在资料中仍可能存在，零隔离不等于气候质量认证；八年变化只能支持该资料范围的年际描述。基于覆盖阈值、H1完整性和已知站点的下游筛选会改变可评价样本范围，B/C须另报告筛选损失。P1-6没有替代模型性能评估。

AI声明建议（由组员提交前核对并补充）：A使用OpenAI Codex辅助分工细化、代码与文档编写、质量检查及复现流程。A部分的运行状态、数量和哈希以实际输出核验；模型与分析的AI使用需B/C分别补充，不宣称整组已独立人工逐行审查或已完成所有实验。

机器可读表：outputs/p1_6_support/report_metrics.json、full_run_resources.csv、annual_cleaning_and_targets.csv、loss_ledger.csv。正式数据身份不变；本材料为A复现／报告支持补充，不修改rainfall_v1冻结文件。


## A英文报告章节

### Data and cleaning

The eight annual data.gov.sg rainfall exports for 2017–2024 contain 48,538,713 records according to Spark parsing and occupy 8,047,044,701 bytes. Annual dataset links and local full-file SHA-256 hashes are recorded in `manifests/raw_manifest.json`. Download completeness was confirmed by the user; these local hashes are not presented as server-provided reference checksums. The records contain five-minute rainfall measurements in millimetres, station metadata and event/update timestamps.

We retained the original fields, record text and provenance. For 2017 only, observations whose original epoch remainder modulo 300 was 299 were shifted forward by one second to the five-minute endpoint, while retaining the original event timestamp. This affected 1,775,327 source records and produced 86 pairs of aligned-key collisions with equal rainfall. The original exact-grid observation was retained; rainfall was neither summed nor averaged. Both sides of each collision were preserved in a 172-row lineage table, including two retained observations with metadata conflicts. Conflicting rainfall would quarantine the entire key group, but no such groups occurred in these data.

Finite nonnegative measurements, including genuine zeros and extremes, were retained without arbitrary truncation or rounding to 0.2 mm. Exact cumulative rainfall uses Decimal(38,18); observed source precision required at most five effective decimal places. This storage capacity does not imply additional measurement precision. Missing timestamps remain unknown rather than becoming zero-rainfall observations. The source data have 63 days with no network records, including 4–31 August 2018; their physical cause cannot be inferred from these files.

The cleaning ledger closes as 48,538,713 = 48,538,627 retained observations + 86 merged records + 0 quarantined records. A zero quarantine count does not establish time-series completeness or meteorological quality. The observations table has a unique station/endpoint key and preserves 47,121,647 genuine zero measurements. Rainfall is interpreted as the five-minute interval ending at the aligned timestamp; calendar aggregation for Task 1 uses the interval start, including midnight contributions to the preceding day.

### Future-window truth and completeness

Each observation supplies one prediction anchor. We require observations at all six exact offsets from t+300 to t+1800 seconds for the future 30-minute total. Incomplete totals remain null; complete dry windows contain Decimal zero. History completeness requires all 12 positions from t−3300 to t. Adjacent-year context is included before anchors are selected, preventing artificial truncation at file boundaries.

There are 47,636,833 complete future windows and 901,794 unknown future totals. Historical inputs are complete for 47,347,833 anchors and incomplete for 1,190,794. Both windows are complete for 47,048,872 anchors; further fold-boundary and station-support restrictions remain the responsibility of downstream processing. History and future exclusions overlap and must not be added as disjoint losses. All anchors, including invalid ones, remain in the target table. Threshold labels must use the strict Decimal comparison `future_30m_mm > threshold_mm`; equality is a negative label, while an unknown total is not a negative example.

| Year | Raw records | Observations | Merged records | Future complete | Future unknown | Both windows complete |
|---|---|---|---|---|---|---|
| 2017 | 5,256,106 | 5,256,020 | 86 | 5,057,559 | 198,461 | 4,822,618 |
| 2018 | 4,594,246 | 4,594,246 | 0 | 4,079,741 | 514,505 | 3,981,287 |
| 2019 | 5,026,862 | 5,026,862 | 0 | 4,991,346 | 35,516 | 4,935,783 |
| 2020 | 6,334,473 | 6,334,473 | 0 | 6,292,286 | 42,187 | 6,234,155 |
| 2021 | 7,044,646 | 7,044,646 | 0 | 7,013,455 | 31,191 | 6,976,422 |
| 2022 | 7,133,230 | 7,133,230 | 0 | 7,108,318 | 24,912 | 7,073,178 |
| 2023 | 6,742,890 | 6,742,890 | 0 | 6,719,652 | 23,238 | 6,687,003 |
| 2024 | 6,406,260 | 6,406,260 | 0 | 6,374,476 | 31,784 | 6,338,426 |

E2 role fields support training through 2020/validation in 2021, training through 2021/validation in 2022, and training through 2022/validation in 2023. After configuration selection, the final refit uses 2017–2023 and the final test uses 2024. A generated truth/completeness and role fields without fitting a model, expanding thresholds or learning features. Future totals and future-dependent eligibility flags are prohibited as model inputs or prior inference conditions.

### Computing environment and observed runtime

The local machine uses Windows 11, a 12th Gen Intel(R) Core(TM) i7-12700H CPU (14 physical cores and 20 logical processors), and approximately 31.73 GiB of system-visible physical memory. Python 3.11.5, Spark/PySpark 3.5.7, Py4J 0.10.9.7 and Java 17.0.20.1 were used with the Asia/Singapore session timezone. Full stages used `local[4]`, a configured 4g driver heap and 48 shuffle partitions. Local threads are not cluster nodes, and the heap setting is not peak resident memory.

Original full-run wall times were 31.48 minutes for auditing, 34.27 minutes for cleaning, and 9.82 minutes for target construction, calculated from each stage's original start/finish receipts. They include that stage's lifecycle and built-in checks but exclude dependency installation, separately executed validators and human work between stages. No full-run peak RSS, CPU-utilisation or temporary-disk peak measurements were collected. Formal Parquet payloads occupy 4,678,105,303 bytes, which is not a peak-disk requirement.

### Verification, limitations and AI assistance

Existing full-data validation includes count reconciliation, unique keys, time-grid/schema checks, root-directory readback and file hashes. Target checks include independent Decimal recomputation for 29 real cases covering 522 expected positions. The additional P1-6 replay uses 1,297 preserved raw source records and 114 selected anchors, covering all 86 collisions and the development-period manual cases. Headers/newlines are reconstructed, while record text is unchanged. The frozen production functions are rerun; all cleaned observations, collision lineage and selected targets are compared against full-run reference rows after Parquet writing/reading, and every selected target is independently recomputed by timestamp-key lookup. The initial local replay took approximately 72.58 seconds with `local[2]`, 2g and eight shuffle partitions. It is a functional check, not a representative performance benchmark. A separately extracted support-package replay is documented by its receipt.

Missing coverage limits monthly/annual interpretation and evaluation scope. Alignment and interval interpretation are explicit project assumptions. Metadata first/last appearances do not establish continuous validity periods. Under the retrospective event-time availability assumption, delayed updates are audited but are not predictors; results should not be claimed as demonstrated real-time deployment performance. The eight-year record supports descriptive interannual comparisons, not an established long-term climate trend. A's checks use the same host and configured runtime; B/C and macOS acceptance, and final-model reload/prediction validation, remain pending.

Suggested AI disclosure for A, to be reviewed and supplemented by the team: OpenAI Codex assisted with planning, implementation, documentation, quality checks and reproduction workflows. Reported A-stage counts, hashes and execution status were checked against actual outputs. B/C should disclose their own AI use and verification separately. This draft does not claim that all code has been manually reviewed line by line or that modelling is complete.


## 复现与证据

日常有三个入口：read_data_release检查读写接口，reproduce_a_sample重跑真实原始片段，record_result_metadata记录实际下游结果来源。首次两项A本机验证通过，不代表B/C／macOS已经完成实机验收。P1-5接口样本不是训练代表样本；P1-6片段包含114个选定锚点所需的实际上下文，缺槽保留。

原始全量质量证据在outputs/p0_2_audit/p0_2_v1、outputs/p0_3_clean/p0_3_v1、outputs/p0_4_targets/p0_4_v1；A资源与数量表在outputs/p1_6_support。原收据保留原发布标识，本次交接修订的验证在outputs/team_handoff。processing_status是当前进度，release_manifest是当前交付身份；不要运行旧阶段发布脚本覆盖当前说明和进度。

旧的全量入口和校验脚本有固定v1路径及签名依赖。完整重建须在独立工作副本逐阶段生成新证据及清单；仅改--source/--output不是通用全量runner。当前正式CSV／Parquet不在小包中，获取完整数据后保持README路径。GitHub未发布，最终下载渠道和课程提交包还需确认。

data_version只在数据内容／口径变化时提升；本次只修订文档发布标识。config_hash是project.json原始字节SHA-256，实验配置另存experiment_config_hash。新报告、特征、模型记录当前发布标识，旧结果保留其原记录，不静默替换。

## 课程原文

<details>
<summary>展开原课程要求</summary>

The Historical Rainfall across Singapore datasets, available at https://data.gov.sg/collections/2279/viewLinks to an external site., provide rainfall measurements recorded at five-minute intervals by monitoring stations across Singapore. Use the annual datasets from 2017 to 2024. Each record includes the observation time, station information, and rainfall amount (mm).

The project involves structured data processing and machine learning using Apache Spark. You may use any programming language supported by Spark. The two tasks are as follows:

Task 1: Explore weather patterns. Explore the weather patterns reflected in the rainfall data and how they change within a year and across the years 2017–2024. Present and discuss your findings using appropriate analysis and visualizations.
Task 2: Predict rainfall. Build a machine learning model that estimates the probability that a monitoring station will accumulate more than a given amount of rainfall in the next 30 minutes. The model should take the station, prediction time, and rainfall threshold as inputs. Select suitable features and evaluate your model using Spark MLlib.
The project requires you to write a report describing your approach and results. In the report, you must

Explain how you cleaned the data, investigated weather patterns and their changes, selected features, and developed your machine learning model.
Describe your computing environment (e.g., a local machine, Google Cloud, or Databricks), including the relevant VM or cluster configuration.
Present and discuss the results of both tasks, preferably with figures and tables, and evaluate your model’s predictive performance.
Declare any AI usages.
Discuss any additional work, observations, limitations, or insights you would like to share.
This is again a group project, and each group can have at most three people. Please submit the following materials to Canvas no later than 18 Oct 2026: 

Your complete source code and a README explaining how to run it.
Your trained model.
A PDF report listing the names and student IDs of all group members.
</details>
