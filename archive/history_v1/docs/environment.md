# Computing environment and native Spark setup

Original full A stages used Windows 11, Intel i7-12700H (14 physical cores, 20 logical processors), approximately 31.73 GiB visible RAM, Python 3.11.5, PySpark 3.5.7, Py4J 0.10.9.7 and Java 17.0.20.1. Spark timezone is Asia/Singapore. Full stages used local[4], 4g driver heap and 48 shuffle partitions; sample replay used local[2], 2g/eight partitions. Heap is not peak RSS, and threads are not cluster nodes.

Use requirements-spark.txt and scripts/setup_java17.py. Windows native winutils/Hadoop DLL components are the pinned community Hadoop 3.3.5 build; Spark's bundled Hadoop Java is 3.3.4. The actual read/write workload passed, but this is not proof of universal compatibility. scripts/setup_hadoop_windows.py records source/version. Set JAVA_HOME, HADOOP_HOME and PATH consistently before Spark starts.

Windows console text requires UTF-8; distinguish display encoding from CSV data encoding. Preserve Unicode paths or percent-encode file URIs correctly. Confirm native DLL architecture, Java major and filesystem permissions before retrying native errors. Record normal Java process exit and write/readback, not only SparkSession creation. Avoid leaving stale child processes on failure. A historical smoke check is not a full performance benchmark.

Current Mac B/C environment is separately recorded: macOS 14.1, Apple M3 Max, 36 GB, Python 3.11.17, Java 17.0.20.1 and Spark 3.5.7. B local[2]/4g/128; C local[2]/6g/128. Memory peaks were not instrumented. See integrated report for measured stage timing and its scope.
