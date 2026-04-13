#!/usr/bin/env python3
import ast
import logging
import re
import subprocess
import time
import requests

from logger_config import get_logger

logger = get_logger(__name__)


class HBaseShellCommandError(Exception):
    pass


class HBaseDockerClient:
    def __init__(self, container_name, hbase_ui_port=16010, cluster_name="HBase Cluster",
                 max_retries=12, sleep_time=5):
        self._container_name = container_name
        self._hbase_ui_port = hbase_ui_port
        self._cluster_name = cluster_name
        self._max_retries = max_retries
        self._sleep_time = sleep_time

    @property
    def name(self):
        return self._cluster_name

    def wait_for_hbase_ui(self):
        """Checks for a 200 OK on the HBase Master UI."""
        url = f"http://localhost:{self._hbase_ui_port}"
        logger.info(f"Waiting for HBase UI: {self._cluster_name} on {url}")
        last_exception = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    logger.info(f"SUCCESS: {self._cluster_name} UI is up.")
                    return True
            except requests.exceptions.ConnectionError as e:
                last_exception = e
            logging.info(f"Waiting {self._sleep_time} seconds before requesting HBase UI again")
            time.sleep(self._sleep_time)

        raise RuntimeError(f"\nTIMEOUT: {self._cluster_name} UI failed to respond after "
                           f"{self._max_retries} attempts. "
                           f"Last raised exception was: {last_exception}")

    def check_server_status(self):
        """Runs 'status' inside the HBase shell and validates the output."""
        logger.info(f"Validating Cluster Status: {self._cluster_name} ({self._container_name})")
        for attempt in range(1, self._max_retries + 1):
            try:
                output = self.get_hbase_status()

                # The cluster's status should have 1 active master, 1 region server,
                # and no dead servers
                validations = {
                    "Active Master": "1 active master" in output,
                    "Region Server": "1 servers" in output,
                    "No Dead Servers": "0 dead" in output
                }

                if all(validations.values()):
                    for check, status in validations.items():
                        logger.info(f"    [PASS] {check}")
                    logger.info(f"SUCCESS: {self._cluster_name} is fully operational.")
                    return True
                else:
                    logger.warning(f"{self._cluster_name} is responding, but not all "
                                   f"components are ready...")
                    logger.info(f"HBase 'status' command output:\n{output}")

            except subprocess.CalledProcessError:
                pass

            logging.info(f"Waiting {self._sleep_time} seconds before getting status on "
                         f"{self.name} again")
            time.sleep(self._sleep_time)

        raise RuntimeError(
            f"\nTIMEOUT: {self._cluster_name} shell check failed after {self._max_retries} attempts.")

    def __run_hbase_command(self, hbase_cmd):
        # In the Terminal, we usually put double quotes around everything after "-c", but doing that
        # with subprocess.run() results in a failure.
        cmd = ["docker", "exec", self._container_name, "bash", "-c",
               f'''hbase shell -n <<< "{hbase_cmd}"''']
        cmd_str = ' '.join(cmd)

        logger.debug(f"Running command on {self._cluster_name}: {cmd_str}")
        process = subprocess.run(cmd, capture_output=True)
        stdout = process.stdout.decode('utf-8')
        if process.returncode != 0:
            raise HBaseShellCommandError(f"The following HBase shell command failed on the "
                                         f"{self._cluster_name} ({self._container_name}): "
                                         f"{hbase_cmd}\nThe docker command used to run this was: "
                                         f"{cmd_str}\nThe shell command's STDERR was:"
                                         f"\n{process.stderr.decode('utf-8')}\n"
                                         f"The shell command's STDOUT was:\n{stdout}\n")
        return stdout

    def create_table(self, table_name, column_family):
        logger.info(f"Creating table '{table_name}' on {self._cluster_name}")
        create_cmd = f"create '{table_name}', '{column_family}'"
        output = self.__run_hbase_command(create_cmd)

        if f"Created table {table_name}" not in output:
            logger.error(f"Could not create table '{table_name}' on {self._cluster_name}")
            return False
        return True

    def list_tables(self):
        """Gets the list of HBase tables and returns it as a Python list"""
        logger.debug(f"Getting the list of tables in HBase on {self.name}")
        pattern = r'\[(.*?)\]'
        output = self.__run_hbase_command("list")
        match = re.search(pattern, output)
        return ast.literal_eval(match.group(0))

    def verify_table_exists(self, table_name):
        logger.debug(f"Verifying '{table_name}' is in the list of tables on {self.name}")
        return table_name in self.list_tables()

    def get_hbase_status(self):
        logger.debug(f"Getting status of {self.name}")
        return self.__run_hbase_command("status")

    def disable_table(self, table_name):
        logger.debug(f"Disabling table '{table_name}' on {self.name}")
        self.__run_hbase_command(f"disable '{table_name}'")

    def drop_table(self, table_name):
        logger.info(f"Dropping table '{table_name}' on {self.name}")
        self.__run_hbase_command(f"drop '{table_name}'")

    def refresh_meta(self):
        logger.debug(f"Refreshing meta on {self.name}")
        self.__run_hbase_command("refresh_meta")

    @staticmethod
    def clean_up_tables(active_cluster, replica_cluster):
        """
        Drops all tables on the active cluster and then runs 'refresh_meta' on the
        read-replica cluster to remove those tables
        """
        tables = active_cluster.list_tables()
        if tables:
            logger.info(f"Removing all existing tables on {active_cluster.name}: {tables}")
            for table in tables:
                active_cluster.disable_table(table)
                active_cluster.drop_table(table)
            logger.info(f"Running 'refresh_meta' on {replica_cluster.name} cluster to sync it with "
                        f"the {active_cluster.name}")
            replica_cluster.refresh_meta()
