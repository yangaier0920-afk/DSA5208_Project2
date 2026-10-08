# P0-1 共享小样本 v1

这是供 A/B/C 核对环境与读取接口的固定样本。来自已通过验证的运行 `20261008_134521_945643`：2017—2024 每年开头 2,000 条记录，共 16,000 条。样本不代表全年天气，也不是正式清洗数据。

整个目录/ZIP 一起交接，不能只复制一个 Parquet 文件。内容包括原始样本 CSV、按 year/month 分区的 Parquet、字段与文件哈希 manifest、版本约定、读取脚本和原始八年资料清单；不包含 8 GB 原始资料和任何 Windows 二进制环境。

## Windows 项目内验收

在已经按项目 README 配置环境的项目根目录执行：

```powershell
$env:PYTHONIOENCODING = 'utf-8'
.\.venv\Scripts\python.exe data\samples\p0_1_v1\read_shared_sample.py --member B --receipt outputs\p0_1_receipts\B.json
```

C 将 `--member B` 和收据文件名改为 C。读取器不需要访问 Raw_Dataset。

## macOS 验收准备

先安装 Python 3.11 和适合本机架构的 Java 17。Intel 与 Apple Silicon 使用对应的 Java 包。创建本机 Python 环境并安装这里的 requirements-spark.txt；不复制 Windows 的 .venv、Java ZIP 或 Hadoop DLL。

在解压得到的 p0_1_v1 目录打开终端，可执行：

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements-spark.txt
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
.venv/bin/python read_shared_sample.py --member B --receipt ../B_read_receipt.json
```

这要求 Java 17 已被 macOS 的 java_home 工具识别。若使用其他 Java 安装方式，将 JAVA_HOME 设置为实际 JDK 17 目录。读取器会确认实际 Java 主版本、Python 次版本及 PySpark/Py4J 版本。

macOS 分支已提供，但尚未在 Mac 实机运行，不能把本机 Windows 的 PASS 当作 Mac 验收。

## 通过标准与反馈

脚本核对交付文件的 SHA-256、Parquet 16,000 行、字段类型、473 组计数与雨量和，以及实际 Python worker 运算。预期最后输出 `PASS` 并生成 JSON 收据。

请 B/C 将实际生成的 JSON 收据交回 A；失败时提供本次错误及 FAIL 收据。未收到真实收据前，团队状态为“待验收”。A 本机读取通过不代替 B/C 的设备验收。

Python 统一 3.11 系列、Java 统一 17 系列，PySpark/Py4J 精确锁定为 3.5.7/0.10.9.7；各人的补丁版本与架构记录在收据中。共享数据采用 Asia/Singapore 时区口径，保留原始时间秒数和雨量精度。
