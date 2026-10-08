# P0-1 验收状态

核查日期：2026-10-08。

**A 的本机环境验证和交付准备已完成；全组 P0-1 尚待 B/C 实机读取确认。**

| 计划要求 | 当前状态 | 证据 |
|---|---|---|
| Python、Java、PySpark 版本约定 | 已形成版本约定，A 已验证；B/C 实际版本待确认 | `configs/environment.lock.json`、`requirements-spark.txt` |
| 小样本读取、聚合、Parquet 保存与重读 | A 本机已通过 | `outputs/spark_smoke/20261008_134521_945643/verification.json` |
| environment.md | 已完成 | `docs/environment.md` |
| 八年文件、大小、哈希、来源 | 独立 manifest 已完成；八个全文 SHA-256 本次再次核对一致 | `manifests/raw_manifest.json` |
| 同一份共享小样本 | 已固定目录并生成 ZIP，CRC 与文件 SHA-256 检查通过 | `data/samples/p0_1_v1/`、`outputs/p0_1_shared_sample_v1.zip` |
| 交付目录里的读取入口 | A 在实际交付副本上读取通过：16,000 行、473 组、类型和 worker 检查 | `outputs/p0_1_receipts/A.json` |
| B/C 各自读取同一小样本 | 待实际执行并返回收据 | `outputs/p0_1_receipts/B.json`、`C.json` 尚未收到 |
| macOS 设备验收 | 已提供独立读取脚本和操作说明；未在 Mac 实机测试 | 共享包 `README.md`；不能用 A 的 Windows PASS 代替 |

原始 manifest 的来源链接对应官方年度数据页面。原下载日期、原始下载请求 URL 未记录，因此保留为 null；SHA-256 用于标识现有本地文件，不能解释为与官方远程文件哈希比对通过。数据物理行数也不等于正式清洗后的有效记录数。

## A 交给 B/C 的内容

将 `outputs/p0_1_shared_sample_v1.zip` 交给 B/C，按包内 README 在各自环境执行，返回 JSON 收据。该包约 134 KiB，不需要另外传送八年原始 CSV 就能进行共享读取验收。这里仅准备交付文件，未代用户向其他成员发送消息或文件。

Python 统一 3.11 系列、Java 统一 17 系列，PySpark/Py4J 精确锁定为 3.5.7/0.10.9.7，时区统一 Asia/Singapore。具体 Python/Java 补丁、系统与架构记录在每人的收据里。

当前 A 收据通过，只能确认 A 本机。收到 B/C 的真实 PASS 收据、确认两者使用同一 sample manifest 与版本约定后，才把全组 P0-1 标为全部完成。

后续进度（2026-10-08）：A 已完成 P0-2 八年 Spark 全量质量审计，见 [p0_2_quality_report.md](p0_2_quality_report.md)。B/C 实机读取验收按用户要求暂缓，仍不能将全组 P0-1 标为全部完成。
