# P1-5：A本地交接与版本管理

当前交接以 [data_contract.md](data_contract.md) 和 `manifests/release_manifest.json` 为入口，正式身份为rainfall_v1／data_version v1；observations保持p0_3_v1，targets保持p0_4_v1。原规则配置文件不改动，config_hash沿用已经通过P0-3/P0-4验收的SHA-256。

## 交付文件

| 内容 | 位置 |
|---|---|
| 统一职责、质量规则、时间与标签口径、运行命令 | `docs/data_contract.md` |
| 五张正式表完整字段字典、类型及用途 | `docs/field_dictionary.md` |
| 全量Parquet／配置／代码／文档／样本SHA-256与相对路径 | `manifests/release_manifest.json` |
| 当前正式接口的八年配对小样本 | `data/samples/p1_5_v1/`，每张表1,309行 |
| B/C统一读写验收入口 | `scripts/read_data_release.py` |
| B/C实际结果版本记录入口 | `scripts/record_result_metadata.py` |
| 本地接口包（不含全量CSV／Parquet、Java或虚拟环境） | `outputs/p1_5_release/rainfall_v1_interface.zip` |
| 接口包字节数／SHA-256 | `outputs/p1_5_release/bundle_manifest.json` |

## 验收状态如何核查

正式状态以 `configs/processing_status.json` 与以下实际收据为准：

- `outputs/p1_5_release/freeze_receipt.json`：核对旧阶段交付字节／哈希后冻结；不重新清洗或生成标签。
- `outputs/p1_5_receipts/A_sample.json`：A实际读取小样本、schema／唯一键／一对一关联／空值／逐年计数、Python worker、Parquet写后重读及Java退出。
- `outputs/p1_5_release/A_relocated_sample.json`：同一Windows电脑、原Java/Python环境下，把接口ZIP解压到新目录（含空格），从另一工作目录运行reader，核验路径可迁移。这个测试不代替macOS或B/C实机测试。
- `outputs/p1_5_release/validation.json`：A本地交付检查结果，另测试配置被改、未登记Parquet、路径越界会被拒绝，以及下游结果元数据入口。
- `outputs/p1_5_release/A_result_metadata_example.json`：A接口验证结果的真实元数据示例，input_scope=sample、无训练；不是B/C结果。对应计数、实验配置与原始元数据保留在 `outputs/p1_5_release/A_provenance_example/`。

B/C实机验收按用户此前安排仍暂缓，没有代填B/C收据。A可把本地交付标为LOCAL_HANDOFF_READY；“全组已读取同一版本”需B/C真正运行后才成立。C训练状态保持NOT_STARTED。

## 本阶段复现顺序

首次构建新版本才执行sample提取、清单冻结和打包；已有版本拒绝覆盖。日常B/C只需reader及结果元数据入口。构建脚本中的v1是本次固定交付，后续v2应复制／修改新版本路径并重新验收，不原地改v1。

```powershell
.\.venv\Scripts\python.exe scripts\run_spark.py scripts\prepare_release_sample.py
.\.venv\Scripts\python.exe scripts\prepare_data_release.py
.\.venv\Scripts\python.exe scripts\read_data_release.py --member A
.\.venv\Scripts\python.exe scripts\verify_release_handoff.py
```

## GitHub后续发布

用户要求全部内容最终确定后再发布，当前尚未创建仓库、提交、推送或上传。`.gitignore`已将Raw_Dataset、大型正式Parquet、运行环境、ZIP与本机交接收据排除出普通Git提交，保留代码、契约、配置、发布清单、质量证据和小样本。

接口小包让B/C先验证读写；正式分析训练仍需另外下载完整大表，并恢复release_manifest中的相对路径。最终发布前再确定仓库及大表下载渠道、包清单与版本标签，届时填入明确下载地址并生成新的发布快照。未提供全量下载地址时不能把仓库写成“已可完整复现／训练”。A下一项P1-6为复现和清洗报告支持，B/C继续各自分析、特征和E2实验。
