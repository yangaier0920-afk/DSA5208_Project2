# 正式数据逐字段字典（rainfall_v1）

所有timestamp按Asia/Singapore显示，epoch为Unix秒。Parquet的nullable是存储schema属性，并不表示每个字段实际允许缺失；观测有效性及标签空值约束见data_contract。原始与审计字段保留用于追溯，不能整表直接传入模型。

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
