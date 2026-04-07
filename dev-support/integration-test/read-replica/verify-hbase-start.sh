#!/usr/bin/env bash
set -x
source "$(dirname ${0})/.env"
SLEEP_TIME=5
MAX_RETRIES=12

wait_for_hbase_ui() {
  echo "Waiting for HBase UI to be available on ${2}"
  attempts=1
  until $(curl --output /dev/null --silent --head --fail "http://localhost:${1}"); do
    echo -n "."
    sleep "${SLEEP_TIME}"
    ((attempts++))
    if [ ${attempts} -gt $MAX_RETRIES ]; then
      echo "Timeout while waiting for HBase UI on ${2}"
      exit 1
    fi
  done
  echo "HBase UI is up on ${2}"
}

check_server_status() {
  echo "Attempting to get cluster status from HBase shell for ${1}"
  attempts=1
  until docker exec ${1} bash -c "echo \"status\" | hbase shell -n"; do
    echo -n "."
    sleep "${SLEEP_TIME}"
    ((attempts++))
    if [ ${attempts} -gt 6 ]; then
      echo "Timeout while waiting for HBase server status on ${1}"
      exit 1
    fi
  done

  hbase_status=$(docker exec ${1} bash -c "echo \"status\" | hbase shell -n")

  if grep -q "1 active master" <<< ${hbase_status}; then
    echo "Active master is up for ${1}"
  else
    echo "Active master is not up for ${1}"
    exit 1
  fi

  if grep -q "1 servers" <<< ${hbase_status}; then
    echo "Region server is up for ${1}"
  else
    echo "Region server is not up for ${1}"
    exit 1
  fi

  if grep -q "0 dead" <<< ${hbase_status}; then
    echo "No dead servers for ${1}"
  else
    echo "There are dead servers for ${1}"
    exit 1
  fi
}

wait_for_hbase_ui ${ACTIVE_CLUSTER_PORT} "Active Cluster"
# wait_for_hbase_ui ${REPLICA_CLUSTER_PORT} "Read Replica Cluster"
check_server_status ${HBASE_CONTAINER_NAME}
# check_server_status ${HBASE_CONTAINER_NAME}-2
