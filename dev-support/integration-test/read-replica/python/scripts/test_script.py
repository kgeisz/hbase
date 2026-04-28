#!/usr/bin/env python3

from dotenv import load_dotenv
from python.src.environment_loader import get_env
from python.src.hbase_docker_client import HBaseDockerClient
from python.src.logger_config import get_logger

logger = get_logger(__name__)

if __name__ == '__main__':
    # Load settings from .env file
    load_dotenv()
    container_name = get_env("HBASE_CONTAINER_NAME")
    table_name = "t1"
    column_family = "cf"
    column = f"{column_family}:c1"

    active_cluster = HBaseDockerClient(container_name=container_name,
                                       local_conf=get_env('ACTIVE_CLUSTER_CONF'),
                                       hbase_ui_port=get_env('ACTIVE_CLUSTER_PORT'),
                                       cluster_name="Active Cluster")

    logger.info(f"Created HBaseDockerClient with name {active_cluster.name}")
