# 数据交接契约：rainfall_v1（P1-5）

这是A交给B/C的统一入口。正式底座采用 `data_version=v1`、`release_id=rainfall_v1`，绑定 `p0_3_v1` observations 和 `p0_4_v1` targets。身份以 `manifests/release_manifest.json` 为准；阶段验收和本机交接验收以各自收据为准。本次仅冻结本地交付，GitHub发布按用户要求等待最终确认。

## 1. 先读什么、谁继续做什么

| 成员 | 输入与工作 | 必须交付的标识 |
|---|---|---|
| A | 全量审计、清洗、真实未来累计量与完整性、数据契约、版本及复现支持；不训练模型 | 本契约、release_manifest、字段字典、接口样本和A读取收据 |
| B | 用observations和覆盖表做Task 1；从截至t的历史观测生成Task 2特征 | features/图表来源表、result_metadata.json；关联键唯一 |
| C | 按E2角色和窗口资格关联B特征；阈值展开、训练、验证、概率预测及最终测试 | 实验配置、模型及结果、result_metadata.json；配置锁定记录 |

课程固定目标是站点未来30分钟累计雨量**严格超过输入阈值**的概率。A提供实际累计量和可核验范围，C训练概率模型。H1、E2、覆盖90%等是本组已确认的实施选择，不是额外课程硬性要求。

## 2. 正式路径、规模与关联

所有路径相对于下载／解压后的项目根目录；不要复制A电脑的绝对路径。读取Parquet表根目录，Spark自动识别year/month分区。原始CSV和大表不在接口小包中。

| 表 | 正式路径 | 行数／用途 |
|---|---|---|
| observations | `data/processed/p0_3_v1/observations` | 48,538,627；主键station_id + slot_end_epoch_s |
| quarantine | `data/processed/p0_3_v1/quarantine` | 0；空表仍保留schema，不等于没有资料缺口 |
| collision_lineage | `data/processed/p0_3_v1/collision_lineage` | 172；86组碰撞的保留与合并双方记录 |
| station_metadata_history | `data/processed/p0_3_v1/station_metadata_history` | 按站点／元数据组合及来源年份汇总历史；不是连续的坐标有效期表，不能用于未来坐标回填 |
| targets | `data/processed/p0_4_v1/targets` | 48,538,627；主键station_id + prediction_epoch_s，与observations一对一 |

关联条件：`observations.station_id = targets.station_id` 且 `slot_end_epoch_s = prediction_epoch_s`。关联前后检查唯一键、行数与未匹配数，不能仅用站点或日期关联。逐字段名称、Spark类型、空值语义和用途见 [field_dictionary.md](field_dictionary.md)，完整机器schema同时保存在release_manifest中。

## 3. 必须沿用的质量与时间规则

| 规则 | B/C操作约束 |
|---|---|
| 时间与单位 | Asia/Singapore；epoch字段为秒，雨量为毫米；slot_end_ts是该5分钟雨量区间的末端 |
| 2017对齐 | 仅2017原时间余数299的记录+1秒；原始时间保留。不要再次平移或按行号对齐 |
| 碰撞与雨量 | 同键相等雨量保留原网格记录；不等雨量整组隔离。有限非负值、真实零、极端值保留，不按0.2mm四舍五入 |
| 缺测 | 稀疏观测，未知不补零；63个全网整日缺口保留为资料缺测，不能据此断言气象原因 |
| Task 1日期 | 按rain_period_start_ts所属日历累计；00:00末端槽属于前一天。按年月裁剪前要读入所需边界上下文 |
| Task 1比较 | 固定38共同候选站；同月跨年使用覆盖≥90%的合格站点交集，95%作敏感性；站点等权；不可把覆盖筛选施加到全部observations或Task 2 |
| 未来累计 | 必须t+300、600、900、1200、1500、1800秒六槽齐全。无效累计=null，有效无雨=Decimal零 |
| 历史完整性 | H1检查t-3300至t的12槽；历史特征由B另算，不可用未来六槽、未来缺测标记或资格字段作为预测特征 |
| 标签 | 以Decimal(38,18)比较future_30m_mm > threshold_mm；相等为0，无效窗口不展开为负例 |
| 元数据与时效 | 保留原站号（含S113），不把未来最新位置回填历史；本组采用回溯事件时间可用假设，更新时间只供审计 |

详细证据：[P0-3契约](p0_3_data_contract.md)、[P0-3报告](p0_3_quality_report.md)、[P0-4契约](p0_4_data_contract.md)、[P0-4报告](p0_4_quality_report.md)。原始48,538,713 = 正式48,538,627 + 合并86 + 隔离0。targets保留所有观测锚点，其中未来有效47,636,833、未来未知901,794；训练／评价窗口基础合格47,048,872，仍须应用轮次和站点支持范围。

## 4. E2与信息隔离

| 用途字段 | TRAIN | VALIDATION／TEST |
|---|---|---|
| fold1_role | 2017—2020 | 2021 VALIDATION |
| fold2_role | 2017—2021 | 2022 VALIDATION |
| fold3_role | 2017—2022 | 2023 VALIDATION |
| final_role | 配置锁定后2017—2023重训 | 2024 TEST，仅最终评估 |

监督训练／评价先筛相应role，再筛model_anchor_eligible；EXCLUDED_BOUNDARY不能用于该段拟合或评价。year_boundary_safe是诊断字段，不能直接删掉所有跨日历年记录。已知站点编码、标准化等只在各轮训练分区拟合。

阈值候选0、0.5、1、2、5mm的最终支持范围由C仅用2017—2020确定并冻结；选择指标按冻结阈值等权，再按三年验证年等权平均Brier。训练锚点1%流程验证、10%正式采样、之后阈值展开由C实施；验证／测试尽量全量。A没有执行这些训练步骤。

实际预测接口只检查已有历史、已知站点、时间网格和阈值支持范围，不能要求未来标签已经存在。future_30m_mm、future_n_valid、future_missing_offsets_s、target_valid、invalid_reason、model_anchor_eligible及边界诊断属于标签／评价信息，禁止进入模型输入；role字段用于分区过滤。

## 5. 先验接口，再接全量

接口样本位于 `data/samples/p1_5_v1/`：八年S08一月最早96条观测，加上既有29个2017—2020人工案例前后各60分钟内的实际观测；按主键去重，targets直接取正式表对应记录。样本规模、逐年计数和哈希见sample_manifest及release_manifest。它不是训练／评估代表样本；部分锚点的周边观测不完整，不能用这个小包重建全量标签。

Windows（已有本项目环境，在根目录）：

```powershell
.\.venv\Scripts\python.exe scripts\read_data_release.py --member B
.\.venv\Scripts\python.exe scripts\read_data_release.py --member C
```

macOS（本机安装Java 17、Python 3.11；无需Windows的winutils）：

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-spark.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
python scripts/read_data_release.py --member B
```

上述只是B/C各自执行的命令，A不会代填B/C收据。默认只读接口样本，自动校验配置、发布文件哈希、schema、唯一键、一对一关联、未知累计量、逐年计数、Python worker与Parquet写后重读。收据在 `outputs/p1_5_receipts/{成员}_sample.json`。已存在的收据拒绝覆盖，复跑用 `--receipt outputs/p1_5_receipts/B_sample_run2.json`。

获取全部正式Parquet并恢复清单中的目录结构后，可加 `--scope full`；会校验全量文件SHA-256并核对48,538,627个键，所需时间与资源比小样本大。不提供大表的包不应运行full模式。Windows/macOS使用同一配置、版本、时区和Parquet，不需要改变数据逻辑；macOS实机验证仍以实际成员收据为准。

在B/C脚本中使用统一读取函数（该例须有完整数据）：

```python
import sys
from pathlib import Path
root = Path(__file__).resolve().parents[1]  # 此例为根目录下一级脚本
sys.path.insert(0, str(root / 'scripts'))
from data_release import read_tables, provenance
tables = read_tables(spark, root, scope='full')  # 每次运行先校验；时区自动固定
observations, targets = tables['observations'], tables['targets']
data_identity = provenance(root)
```

首次校验后同一进程可使用已经读入的DataFrame；`verify=False`仅用于同一次已验证、输入保持不变的运行，不应绕过下载后校验。采样、小样本验证与正式全量结果须注明input_scope。

## 6. 下游结果如何记版本

每个实际结果目录保存 `result_metadata.json`，包括data_version、config_hash、release_id、release_manifest_sha256、run_id、成员、运行命令、input_scope、experiment_config_hash及结果文件哈希。`config_hash`是A的 `configs/project.json` 原始文件字节SHA-256；B/C实验参数另存，不编辑它，也不能把实验参数哈希写成数据配置哈希。

结果和实验配置已经存在后执行：

```powershell
.\.venv\Scripts\python.exe scripts\record_result_metadata.py --member B --run-id features_001 --input-scope full --result-dir outputs/B/features_001 --experiment-config configs/B_features.json --command "python src/B_features.py --config configs/B_features.json"
```

这是用法示例，不表示上述B配置和结果已经存在。C用相同方式记录真实模型／评价输出。命令会先校验输入版本和文件，再生成元数据，拒绝覆盖已有run。参数文件须随结果保留；模型、特征和报告中的版本号与元数据保持一致。A准备并验证入口，B/C实际产出与标记由各自执行。

## 7. 冻结、变更和发布约定

- v1冻结后不原地改正式Parquet、规则配置、字段说明或发布清单。改变原始输入、清洗口径、标签语义、schema或数据字节，创建v2及新路径，更新契约后重新验收；保留旧版以复现旧结果。
- 仅调B/C特征或模型参数，保持data_version/config_hash，换run_id与experiment_config_hash。仅文档修订也应产生新的release_id和清单快照；数据相同可沿用data_version，不能静默修改已发包。
- 发布清单只使用相对路径，包含每个正式Parquet的大小与SHA-256；Hadoop的.crc及_SUCCESS是运行侧文件，不是跨平台数据身份。不要额外放入未登记Parquet，否则统一校验会拒绝读取。
- `configs/processing_status.json`、成员新收据、P1-5验收结果属于进度记录，不纳入冻结清单，避免状态更新使数据身份循环变化。生成清单的脚本默认拒绝覆盖。
- 本地接口ZIP只含代码、配置、契约、质量证据和小样本；全量CSV／Parquet另行分发并保持原路径。GitHub仓库、下载地址、全量包切分及最终发布标签在最终确认后落实；当前没有远程仓库／下载链接，不将本地冻结写成已发布。
- 不用旧 `validate_p0_3_outputs.py` 覆盖新阶段状态。交接只读校验用read_data_release；P0-3/P0-4的历史验收文件保持不动。

解压后可从任意目录运行reader的绝对脚本路径；项目根目录由脚本位置确定，也可显式传 `--root`。无需全量数据即可验小包，完整数据校验另走full模式。发布准备方法与A的本地验收见 [p1_5_status.md](p1_5_status.md)。
