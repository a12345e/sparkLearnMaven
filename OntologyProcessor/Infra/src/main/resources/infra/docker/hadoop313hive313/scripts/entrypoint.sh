#!/usr/bin/env bash
#
# One entrypoint, four roles. Usage: entrypoint.sh <role> [args...]
#
#   namenode     HDFS NameNode      (formats the name dir on first start)
#   datanode     HDFS DataNode
#   metastore    Hive Metastore     (initialises the schema on first start)
#   hiveserver2  HiveServer2
#
# Anything else is exec'd verbatim, so `docker compose run hive bash` works.

set -euo pipefail

log() { echo "[entrypoint] $*"; }

# Config lives in a bind-mounted /conf so a change is a restart, not a rebuild.
# Hadoop's config dir also holds shipped *-env.sh files, so we copy in rather
# than mounting over it.
install_conf() {
    shopt -s nullglob
    local f
    for f in "${HDFS_CONF_DIR}"/*.xml; do
        cp "$f" "${HADOOP_CONF_DIR}/"
        cp "$f" "${HIVE_CONF_DIR}/"
    done
    shopt -u nullglob

    # Hive resolves Tez settings from its own conf dir only.
    if [ -f "${HDFS_CONF_DIR}/tez-site.xml" ]; then
        sed -i "s|@TEZ_LOCAL_MODE@|${TEZ_LOCAL_MODE:-true}|g" "${HIVE_CONF_DIR}/tez-site.xml"
    fi
}

wait_for() {
    local host=$1 port=$2 name=$3 waited=0 timeout=${WAIT_TIMEOUT:-180}
    log "waiting for ${name} at ${host}:${port}"
    until nc -z "$host" "$port" 2>/dev/null; do
        waited=$((waited + 2))
        if [ "$waited" -ge "$timeout" ]; then
            log "gave up waiting for ${name} after ${timeout}s"
            return 1
        fi
        sleep 2
    done
    log "${name} is up"
}

# Hadoop refuses to start a daemon whose pid file names a live process:
#   "namenode is running as process 1. Stop it first."
# Even in the foreground, `hdfs namenode` goes through hadoop_daemon_handler,
# which makes that check. The pid file lives in /tmp *inside the container*, so
# it survives a container restart - and it always says 1, because that is this
# script. PID 1 is alive again in the restarted container, so the check matches
# and the daemon exits; with `restart: unless-stopped` that is a crash loop that
# never resolves. Docker Desktop restarting, or a reboot, is enough to trigger it.
#
# The process the file referred to died with the old container, so the file is a
# leftover rather than a lock, and removing it here is safe: one container runs
# one daemon, and nothing else can be holding it.
clear_stale_pids() {
    local dir=${HADOOP_PID_DIR:-/tmp}
    log "clearing any pid files left by a previous run in ${dir}"
    rm -f "${dir}"/hadoop-*.pid "${dir}"/yarn-*.pid "${dir}"/hive-*.pid 2>/dev/null || true
}

# HDFS scratch and warehouse directories, created once the NameNode is live.
provision_hdfs() {
    log "provisioning HDFS directories"
    hdfs dfsadmin -safemode wait > /dev/null
    hdfs dfs -mkdir -p /tmp /user/hive/warehouse /tmp/hive
    hdfs dfs -chmod -R 1777 /tmp
    hdfs dfs -chmod -R 1777 /user/hive/warehouse
}

role=${1:-namenode}
shift || true

install_conf
clear_stale_pids

case "$role" in
  namenode)
    if [ ! -d /hadoop/dfs/name/current ]; then
        log "formatting a fresh NameNode (cluster ${HDFS_CLUSTER_NAME:-ontology})"
        hdfs namenode -format -nonInteractive -clusterId "${HDFS_CLUSTER_NAME:-ontology}"
    fi
    exec hdfs namenode "$@"
    ;;

  datanode)
    exec hdfs datanode "$@"
    ;;

  metastore)
    wait_for "${POSTGRES_HOST:-postgres}" "${POSTGRES_PORT:-5432}" "postgres"
    wait_for "${NAMENODE_HOST:-namenode}" "${NAMENODE_PORT:-8020}" "namenode"
    provision_hdfs

    if ! schematool -dbType postgres -info > /dev/null 2>&1; then
        log "initialising the Hive ${HIVE_VERSION} metastore schema in postgres"
        schematool -dbType postgres -initSchema
    else
        log "metastore schema already present"
        schematool -dbType postgres -upgradeSchema > /dev/null 2>&1 || true
    fi

    exec hive --service metastore "$@"
    ;;

  hiveserver2)
    wait_for "${METASTORE_HOST:-metastore}" "${METASTORE_PORT:-9083}" "hive metastore"
    exec hive --service hiveserver2 "$@"
    ;;

  *)
    exec "$role" "$@"
    ;;
esac
