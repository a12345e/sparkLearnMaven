# Hadoop HDFS 3.1.3 + Hive 3.1.3

A local, single-node stack for developing against a real HDFS and a real Hive
metastore. Not a production topology: one DataNode, no Kerberos, no YARN.

## Start

```powershell
cd ~/.ontology-processor/docker/hadoop313hive313   # written there by DockerAssets
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
| `datanode` | HDFS DataNode | 9864 (UI), 9866 (block transfer) |
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

Every container in the stack has the Hive client installed and a default JDBC
URL configured (`conf/beeline-site.xml`), so a bare `hive` connects to
HiveServer2 from any of them — including the namenode:

```powershell
docker compose exec namenode bash     # then, inside the container:
hive
```

```
0: jdbc:hive2://hiveserver2:10000/default>
```

Hive 3 still has the legacy `hive` CLI, but it is deprecated and gone in
Hive 4, so prefer Beeline. `conf/beeline-site.xml` gives it a default URL, so
a bare `beeline` connects instead of opening a prompt reading
`No current connection`. The explicit form works too:

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

Reading or writing a byte needs one more setting, because block IO is a second
connection. The NameNode only says *which* DataNodes hold the blocks; the
client then opens its own socket to one of them. So:

```java
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
```

Without it a client here dials the DataNode's internal docker IP, which is not
routable from the host, and the transfer hangs and then fails - *after* the
NameNode call succeeded, which is why it tends to look like a Spark bug. With
it the client dials the name the DataNode registered under instead.

That name is `DATANODE_ADVERTISED_HOST` in `.env`, and it is `localhost`, which
resolves to the published 9866. Nothing needs to go in your hosts file. The
containers are unaffected: they keep
`dfs.client.use.datanode.hostname=false` from `conf/hdfs-site.xml` and dial the
internal IP, so one DataNode serves both networks. Set the variable to
`datanode` instead if every client of yours runs on the compose network.

`HadoopStack.sparkHadoopOptions()` returns both settings, and
`HdfsDirectOpsTest` exercises them against the running stack.

Two more details that are already handled:

- The NameNode and the DataNode both bind `0.0.0.0`, so the published ports
  actually reach them.
- `docker compose restart datanode` is not enough to pick up a conf change and
  in fact leaves the container crash-looping on a stale pid file
  (`datanode is running as process 1. Stop it first.`). Recreate it instead:
  `docker compose up -d --force-recreate datanode`.

Spark 3.5 ships Hive 2.3.9 metastore client jars, and its isolated client
loader knows Hive 0.12 through 3.1 — nothing above. Hive 3.1.3 is therefore
the newest metastore this project can name: set
`spark.sql.hive.metastore.version=3.1.3` with
`spark.sql.hive.metastore.jars=maven` (or a jars path) and Spark loads that
client in its own classloader. The built-in 2.3.9 client also works for plain
tables. `ClusterConfig` and `InitializeInfraTest` both use 3.1.3.

## Execution engine

Queries run on **MapReduce, in-process**, because there is no YARN here.

Tez is installed — Apache Tez 0.10.1, the release built against Hadoop 3.1.3,
with its jars on Hive's classpath, since the Hive distribution ships only
`tez-api` and not the runtime — but it is *not* the default engine at Hive
3.1.3. Hive 3.1.3 derives its split count from the resource Tez advertises,
and in local mode that value comes back nonsensical, so every query fails in
`FileInputFormat.getSplits` with `Illegal Capacity: -1547`. Hive 4.0.0 guards
against this, which is why the stack ran on Tez before it was pinned to 3.1.3.
Switch `hive.execution.engine` back to `tez` when pointing at real YARN. Its own jars go in wholesale; its third-party
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
| `conf/beeline-site.xml` | default JDBC URL, so a bare `hive` connects |
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
- Java 8 throughout — what Hadoop 3.1.3 and Hive 3.1.3 are tested against, and
  what this project's Maven build targets.
- Everything runs as root and `dfs.permissions.enabled=false`. Local
  development only.
