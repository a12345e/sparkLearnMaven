# Hadoop HDFS 3.3.6 + Hive 4.0.0

A local, single-node stack for developing against a real HDFS and a real Hive
metastore. Not a production topology: one DataNode, no Kerberos, no YARN.

## Start

```powershell
cd OntologyProcessor/docker
docker compose up -d --build
```

The first build downloads ~1.2 GB of Apache tarballs and takes a few minutes.
Afterwards `docker compose up -d` starts in under a minute.

Startup is ordered by health checks, so `up -d` returns before HiveServer2 is
serving. Wait for it with:

```powershell
docker compose ps                      # all services "healthy"
docker compose logs -f hiveserver2     # or watch it come up
```

## What runs

| Service | Role | Host ports |
| --- | --- | --- |
| `namenode` | HDFS NameNode | 9870 (UI), 8020 (RPC) |
| `datanode` | HDFS DataNode | 9864 (UI) |
| `postgres` | Metastore database | 5432 |
| `metastore` | Hive Metastore | 9083 (thrift) |
| `hiveserver2` | HiveServer2 | 10000 (JDBC), 10002 (UI) |

- NameNode UI — <http://localhost:9870>
- HiveServer2 UI — <http://localhost:10002>

The four Hadoop/Hive services are one image (`Dockerfile`) with the role passed
as the command; `scripts/entrypoint.sh` dispatches on it. First start of the
NameNode formats the name directory, and first start of the metastore runs
`schematool -initSchema`; both are idempotent afterwards.

## Query it

```powershell
docker compose exec hiveserver2 beeline -u "jdbc:hive2://localhost:10000/default"
```

```sql
CREATE TABLE demo (id INT, name STRING);
INSERT INTO demo VALUES (1, 'ada'), (2, 'grace');
SELECT * FROM demo;
```

From the host, over JDBC: `jdbc:hive2://localhost:10000/default`, no auth,
any username.

`scripts/smoke-test.sql` does the same round trip — create, insert, count,
ordered read, drop — as one check that the metastore, HDFS and Tez are all
working:

```powershell
docker compose exec -T hiveserver2 `
  beeline -u jdbc:hive2://localhost:10000/default -f /scripts/smoke-test.sql
```

(`conf/` and `scripts/` are mounted into every container at `/conf` and
`/scripts`.)

## HDFS from the shell

```powershell
docker compose exec namenode hdfs dfs -ls /
docker compose exec namenode hdfs dfs -put /etc/hostname /tmp/
docker compose exec namenode hdfs dfsadmin -report
```

## Connecting Spark to it

Spark 3.5 talks to this metastore directly. From the host:

```java
SparkSession.builder()
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
    .getOrCreate();
```

Two details make this work from outside the compose network:

- `dfs.client.use.datanode.hostname=true`, so the NameNode hands out
  `datanode:9864` rather than a container IP. Add `127.0.0.1 datanode namenode`
  to your hosts file, or run Spark on the compose network.
- The NameNode binds `0.0.0.0`, so the published ports actually reach it.

Spark 3.5 ships Hive 2.3 metastore client jars. It can talk to a Hive 4
metastore for reads and writes of plain tables; for full Hive 4 semantics set
`spark.sql.hive.metastore.version` and point
`spark.sql.hive.metastore.jars` at the Hive 4 jars.

## Execution engine

Hive 4 removed the MapReduce engine, so queries run on Tez — and the Hive
4.0.0 distribution ships only `tez-api`, not the runtime. The image therefore
installs Apache Tez 0.10.3 (the release built against Hadoop 3.3.6) and puts
its jars on Hive's classpath. Its own jars go in wholesale; its third-party
dependencies only where neither Hive nor Hadoop already ships that artifact,
so no library ends up on the classpath twice.

There is no YARN cluster here, so Tez runs in **local mode** — the DAG executes
inside the HiveServer2 JVM (`conf/tez-site.xml`, `tez.local.mode`). That is
fine for development-sized data and is what keeps the stack to five containers.
Beeline still prints `Executing on YARN cluster with App id application_...`
in local mode; the ID is synthetic and there is no ResourceManager behind it.

To point it at a YARN cluster of your own, set `TEZ_LOCAL_MODE=false` in `.env`
and add the cluster's `yarn-site.xml` plus a `tez.lib.uris` to `conf/`. The
full Tez distribution stays at `/opt/tez` in the image, so
`/opt/tez/share/tez.tar.gz` is the tarball to upload to HDFS for that.

## Configuration

`conf/*.xml` is bind-mounted into every container at `/conf` and copied into
both `$HADOOP_CONF_DIR` and `$HIVE_CONF_DIR` at startup — so editing a config
file needs a `docker compose restart <service>`, not a rebuild.

| File | Covers |
| --- | --- |
| `conf/core-site.xml` | `fs.defaultFS`, proxyuser rules |
| `conf/hdfs-site.xml` | replication, data dirs, bind hosts |
| `conf/hive-site.xml` | metastore JDBC, warehouse, HiveServer2, ACID |
| `conf/tez-site.xml` | local-mode execution |
| `.env` | versions, host port mappings, heap |

Versions are build args (`HADOOP_VERSION`, `HIVE_VERSION`, `TEZ_VERSION`), so
changing them in `.env` needs `docker compose up -d --build`.

## State

Three named volumes hold everything: `namenode-data`, `datanode-data`,
`metastore-db`. They survive `docker compose down`.

```powershell
docker compose down          # stop, keep the data
docker compose down -v       # stop and wipe HDFS + the metastore
```

Wiping is the fix if the NameNode and DataNode ever disagree on cluster ID
(the DataNode logs `Incompatible clusterIDs`), which happens if one volume is
removed without the other.

## Notes

- Both images ship their own Guava; the build keeps the newer of the two in
  both trees, which is what avoids the usual `NoSuchMethodError` from
  `Preconditions.checkArgument`.
- Java 8 throughout — what Hadoop 3.3.6 and Hive 4.0.0 are tested against, and
  what this project's Maven build targets.
- Everything runs as root and `dfs.permissions.enabled=false`. Local
  development only.
