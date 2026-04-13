#!/usr/bin/env python3
import logging

from dotenv import load_dotenv
from environment_loader import get_env
from hbase_docker_client import HBaseDockerClient

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)-5s %(module)s.%(funcName)s(%(lineno)d): '
                               '%(message)s',
                        level=logging.INFO)
    # Load setting from .env file
    load_dotenv()
    active_port = get_env('ACTIVE_CLUSTER_PORT')
    replica_port = get_env('REPLICA_CLUSTER_PORT')
    container_base = get_env('HBASE_CONTAINER_NAME')

    active_cluster = HBaseDockerClient(container_name=container_base,
                                       hbase_ui_port=active_port,
                                       cluster_name="Active Cluster")
    replica_cluster = HBaseDockerClient(container_name=f"{container_base}-2",
                                        hbase_ui_port=replica_port,
                                        cluster_name="Read-Replica Cluster")

    active_cluster.wait_for_hbase_ui()
    active_cluster.check_server_status()

    replica_cluster.wait_for_hbase_ui()
    replica_cluster.check_server_status()

    logger.info("\n" + "=" * 40)
    logger.info("ALL CLUSTERS VERIFIED AND READY")
    logger.info("=" * 40)
