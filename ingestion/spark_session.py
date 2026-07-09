"""
SparkSessionFactory — tạo và quản lý Spark session cho toàn bộ project.

Tại sao cần factory thay vì tạo SparkSession trực tiếp ở mỗi file?
- DRY: config chỉ define một lần
- Consistency: mọi job đều dùng cùng config (AQE, broadcast threshold, ...)
- Testability: trong unit tests, override factory để trả về local SparkSession
- Singleton: một process chỉ nên có 1 SparkSession — factory enforce điều này
"""

import yaml
import os
from pathlib import Path
from pyspark.sql import SparkSession
import structlog

logger = structlog.get_logger(__name__)

# Project root — tìm config files tương đối từ đây
import os
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).parent.parent))


def _load_spark_config() -> dict:
    """Load spark config từ config/spark.yaml."""
    config_path = PROJECT_ROOT / "config" / "spark.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_spark_session(app_name: str = None, local_mode: bool = False) -> SparkSession:
    """
    Tạo hoặc lấy SparkSession hiện có.

    Args:
        app_name: Tên job — hiện trên Spark Web UI để dễ monitor
                  Nếu None → dùng tên từ config
        local_mode: True → chạy local (testing), False → connect cluster

    Returns:
        SparkSession đã được configure đầy đủ

    Note:
        SparkSession.builder.getOrCreate() — nếu đã có session thì trả về session đó.
        Đây là singleton pattern: không tạo 2 sessions trong cùng 1 process.
    """
    config = _load_spark_config()

    name = app_name or config.get("app_name", "fintech_pipeline")

    # Determine master
    if local_mode or os.getenv("SPARK_LOCAL_MODE", "false").lower() == "true":
        master = "local[*]"
        logger.info("spark_session.local_mode", app=name)
    else:
        master = config.get("master", "local[*]")

    builder = (
        SparkSession.builder
        .appName(name)
        .master(master)
    )

    # Apply tất cả configs từ spark.yaml
    for key, value in config.get("configs", {}).items():
        builder = builder.config(key, str(value))
        logger.debug("spark_config.set", key=key, value=value)

    # Hadoop/HDFS config paths — Spark cần biết namenode address
    hadoop_conf_dir = os.getenv("HADOOP_CONF_DIR", str(PROJECT_ROOT / "docker" / "hadoop"))
    core_site = Path(hadoop_conf_dir) / "core-site.xml"
    hdfs_site  = Path(hadoop_conf_dir) / "hdfs-site.xml"

    if core_site.exists():
        builder = builder.config(
            "spark.hadoop.fs.defaultFS",
            "hdfs://namenode:9000"
        )

    session = builder.enableHiveSupport().getOrCreate()

    # Reduce log verbosity — Spark logs rất nhiều, chỉ để WARNING
    session.sparkContext.setLogLevel("WARN")

    logger.info(
        "spark_session.created",
        app=name,
        master=master,
        version=session.version
    )

    return session


def stop_spark_session(session: SparkSession) -> None:
    """
    Dừng SparkSession sau khi job xong.
    Luôn gọi cái này ở cuối job để giải phóng resources.
    """
    if session:
        app_name = session.conf.get("spark.app.name", "unknown")
        session.stop()
        logger.info("spark_session.stopped", app=app_name)
