# OntologyProcessor

Infrastructure for an Apache Spark ETL model that is developed **test-first**.

- **Java:** 8 (`maven.compiler.source/target = 1.8`)
- **Spark:** 3.5.0 (Scala 2.12 binaries)
- **Hadoop/HDFS:** 3.1.3 client, **Hive:** 3.1.3 client (the `Hadood` module)
- **Elasticsearch:** 8.19.4 (the `Infra` module)
- **Build:** Maven multi-module reactor

## What this is (and isn't)

This project is scaffolding. Each module ships a **contract and nothing else** —
no implementations, no wired application, no job reading production data.

The model is grown **under test**: each element is specified by a test against
small in-memory fixtures, and only the code needed to satisfy that test is
written. Tests are the only caller, so the model's shape stays driven by its
specification rather than by an integration.

## Modules

| Module | Artifact | Contract | Shape |
| --- | --- | --- | --- |
| `OntologyExtract` | `com.example:OntologyExtract` | `Extractor` | `SparkSession → Dataset` |
| `OntologyTransform` | `com.example:OntologyTransform` | `Transformer` | `Dataset → Dataset` |
| `OntologyLoad` | `com.example:OntologyLoad` | `Loader` | `Dataset → sink` |
| `IO` | `com.example:IO` | `DataReader`, `DataWriter`, `DdlOperations`, `DataStore` | named table ↔ `Dataset`, plus DDL |
| `Hadood` | `com.example:Hadood` | `ClusterConfig` | external file → Hadoop/Spark settings |
| `Infra` | `com.example:Infra` | `HadoopStack`, `ElasticStack` | docker stacks → endpoints |

**The ETL stage modules are independent** — none depends on another. A pipeline is
assembled by a caller (today, by a test), not by a compile-time chain:

```java
Dataset<Row> extracted = extractor.extract(spark);
Dataset<Row> shaped    = transformer.transform(extracted);
loader.load(shaped);
```

Extract owns everything source-specific, load owns everything sink-specific, and
transform knows about neither. All versions are declared once in the parent's
`<dependencyManagement>`; modules list artifacts without versions.

`IO` is the one exception, and the one module others are *meant* to depend on. It
holds interfaces and depends on nothing of ours, so a backend module can implement
them without dragging anything else along. The dependency only ever points at `IO`
— never out of it.

### Laziness

`Extractor` and `Transformer` are lazy — they describe work without running it,
so a whole pipeline stays a single Spark plan. `Loader` is the exception and
returns `void`: it is the action that ends the pipeline and actually executes
the plan. Don't `count` or `collect` inside the first two.

All three are `Serializable`, since anything captured in a lambda is shipped to
the executors.

## Where the model lives

`OntologyTransform/src/main/java/com/example/ontologyprocessor/transform/model/`
— empty by design, for now. It sits in the transform stage because that is where
domain shaping happens; extract knows about sources, load knows about sinks, and
neither should know about the model. A model element is normally a
`Transformer`.

## The IO contracts (`IO`)

`IO` states how this project reads rows, writes rows and manages structure, once,
so that every storage backend states it the same way. It ships interfaces and the
small value types they are written in, and depends on Spark and nothing else — no
Hadoop, no Hive, no Elasticsearch client.

| Interface | Shape |
| --- | --- |
| `DataReader` | `(SparkSession, TableRef) → Dataset` |
| `DataWriter` | `(Dataset, TableRef, WriteMode) → void` |
| `DdlOperations` | namespaces and tables: create, drop, exists, list, schema |
| `DataStore` | all three, for a backend that is all three |

They are separate so a caller can ask for exactly what it uses. A method that only
reads should take a `DataReader`: it then cannot drop a table, and a read-only
backend can satisfy it honestly.

The vocabulary:

| Type | Is |
| --- | --- |
| `TableRef` | an optional namespace and a name — a Hive database and table, or an Elasticsearch index |
| `WriteMode` | `APPEND`, `OVERWRITE`, `ERROR_IF_EXISTS`, `IGNORE` |
| `TableSpec` | a `TableRef`, a Spark `StructType` and partition columns: what `createTable` needs |
| `Options` | an immutable string map for whatever only one backend understands |
| `IoException` | unchecked; what a backend throws when it fails |

`TableRef`, `WriteMode` and `TableSpec` are the portable half of a call and every
backend must honour them. `Options` is the escape hatch — `compression` on Parquet,
`es.mapping.id` on Elasticsearch — and the only place backend knowledge is allowed
to appear. An implementation ignores an option it does not recognise rather than
failing, so the same options can be handed to two stores.

### How it relates to the ETL stages

`Extractor` and `Loader` are the same shapes with the source and sink baked in; a
reader and a writer are *told* where instead. That is the whole difference, and
bridging them is a lambda, which is why neither module depends on the other:

```java
Extractor people = spark   -> store.read(spark, TableRef.parse("ontology.people"));
Loader    counts = dataset -> store.write(dataset, TableRef.of("counts"), WriteMode.OVERWRITE);
```

Laziness carries over unchanged: a read is lazy, a write is the action that runs
the plan.

### Writing a backend

No implementation ships here, and none should — an implementation lives in the
module that owns its client library. `Hadood` is where the Hive/HDFS one belongs;
Elasticsearch gets its own module later, with indices instead of tables, no
namespaces, and `OVERWRITE` implemented by recreating an index rather than by
handing a mode to Spark.

Add the contracts to that module (the version comes from the parent):

```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>IO</artifactId>
</dependency>
<dependency>
    <groupId>com.example</groupId>
    <artifactId>IO</artifactId>
    <type>test-jar</type>
    <scope>test</scope>
</dependency>
```

Then implement `DataStore`, and specify it against **`DataStoreContract`** — the
one thing here that is worth more than the interfaces:

```java
public class HiveDataStoreTest extends DataStoreContract {
    protected DataStore newStore() { return new HiveDataStore(ClusterConfig.load()); }
}
```

`DataStoreContract` is what a backend must *do*, written down once. An interface can
declare that `OVERWRITE` replaces rather than appends; only a test can hold a backend
to it. Because the tests live beside the contract rather than beside each backend,
Hive and Elasticsearch are held to the same promises, and a disagreement about what a
promise means shows up as a failure instead of as two backends quietly behaving
differently. A backend with no namespaces overrides `supportsNamespaces()` to return
`false`; the namespace tests then check the quiet no-op behaviour `DdlOperations`
promises rather than being skipped.

`InMemoryDataStore` in the test tree is the reference reading, run both ways — with
namespaces and without. It is there to prove the contract is satisfiable and
self-consistent *before* a real backend is written, so that a failure in a Hive or
Elasticsearch store is a fact about that store rather than about the specification.

## The external cluster (`Hadood`)

`Hadood` is the module that knows about the real Hadoop/HDFS/Hive system. It is
the one module with a live dependency on infrastructure, and it holds **none of
its details**: no hostname, port, warehouse path or credential is compiled in.

`ClusterConfig` reads a properties file from **outside the build**, found via
the `hadood.config` system property or the `HADOOD_CONFIG` environment
variable, and turns it into a Hadoop `Configuration` and a set of Spark
settings. One jar therefore runs against dev, staging and production:

```powershell
spark-submit `
  --conf spark.driver.extraJavaOptions=-Dhadood.config=/etc/ontology/cluster.properties `
  --class com.example.ontologyprocessor.Job Hadood/target/Hadood-1.0-SNAPSHOT.jar
```

```java
ClusterConfig cluster = ClusterConfig.load();
cluster.login();                                    // Kerberos, or the configured user
SparkSession spark = cluster
        .configure(SparkSession.builder().appName("ontology"))
        .getOrCreate();

FileSystem hdfs = cluster.fileSystem();             // the external namenode
```

`Hadood/conf/cluster.properties.example` is the annotated template — copy it,
do not edit it in place. The keys:

| Key | Becomes |
| --- | --- |
| `hdfs.uri` | `fs.defaultFS` / `spark.hadoop.fs.defaultFS` |
| `hadoop.conf.dir` | the cluster's `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml` |
| `hadoop.user.name` | `HADOOP_USER_NAME`, on an unsecured cluster |
| `hive.conf.dir` | the cluster's `hive-site.xml` |
| `hive.metastore.uris` | `hive.metastore.uris`, and turns Hive support on |
| `hive.metastore.version` | `spark.sql.hive.metastore.version` (default `3.1.3`) |
| `hive.metastore.jars`, `hive.metastore.jars.path` | `spark.sql.hive.metastore.jars[.path]` |
| `hive.warehouse.dir` | `spark.sql.warehouse.dir` |
| `security.authentication`, `kerberos.principal`, `kerberos.keytab` | the `UserGroupInformation` login |
| `hadoop.opt.<key>` | `<key>` on the `Configuration`, and `spark.hadoop.<key>` |
| `spark.<key>` | Spark, verbatim — and wins over everything above |

Precedence runs left to right: the cluster's site XML files, then this file's
named keys, then `hadoop.opt.*`, then `spark.*`.

### Why the versions are what they are

- **`hadoop-client-api` / `hadoop-client-runtime` 3.1.3**, not `hadoop-hdfs`.
  `hadoop-hdfs` proper is the *server* — namenode, datanode, Jetty and JSP. A
  client wants the HDFS client classes, and those ship inside
  `hadoop-client-api` alongside `hadoop-common`. Declaring the pair explicitly
  also overrides the 3.3.4 one Spark 3.5.0 pulls in, so exactly one Hadoop
  version is on the classpath — note that at 3.1.3 this is a *downgrade* of
  Spark's own dependency, so a Spark upgrade is what would break it. `hadoop-common` and `hadoop-hdfs-client` are
  pinned to 3.1.3 in `<dependencyManagement>` for anyone who needs the
  unshaded jars instead.

- **Hive 3.1.3 is `provided`, and only `hive-jdbc`.** Spark 3.5 embeds Hive
  2.3.9 for its own internals and pulls `hive-exec`, `hive-metastore` and
  friends in transitively; forcing those to 3.1.3 breaks `spark-hive`, so the
  parent deliberately does *not* manage them. Spark reaches a Hive 3.1.3
  metastore the way it is designed to — `spark.sql.hive.metastore.version` plus
  `spark.sql.hive.metastore.jars`, which load that client in an isolated
  classloader from the path in the config file.

  `ClusterConfig.sparkSettings()` refuses a config that names a metastore
  version Spark cannot reach, rather than letting the job fail on connect.

  3.1.3 is also the ceiling: Spark 3.5 ships isolated clients for Hive 0.12
  through 3.1 and nothing above, so Hive 4 cannot be named here however the
  jars are configured. A newer metastore *server* is fine — Spark only has to
  be able to build a client that can talk to it.

## Local docker stacks (`Infra`)

`Infra` starts the two stacks this project develops against — HDFS + a Hive
metastore, and 1–6 independent Elasticsearch clusters — and tells you where they
ended up. Spark always runs **locally**; these are the external systems it talks
to over published ports.

```java
HadoopStack hadoop = HadoopStack.packaged();
hadoop.ensureUp();                      // starts it only if the ports are shut

ElasticStack elastic = ElasticStack.packaged();
List<String> urls = elastic.ensureUp(1);   // idempotent; already-running clusters are left alone
```

"Is it up" is answered by probing the ports that actually have to work, not by
asking docker what it thinks — and Elasticsearch host ports are ephemeral, so
`url(...)` reads back what docker published rather than assuming a number.

### The compose files ship in the jar

They live in `Infra/src/main/resources/infra/docker/`, which puts them on the
classpath beside the `infra.docker` package, and `DockerAssets` writes them out
to `~/.ontology-processor/docker` on first use. `docker compose` cannot read a
compose file out of a jar, and the hadoop stack bind-mounts `./conf` and
`./scripts` into its containers, so the files have to be on disk and have to
outlive the JVM that put them there — which is why it is not a temp directory.

That packaging is what lets **another project** use these stacks. It adds one
dependency and needs nothing checked out:

```xml
<dependency>
  <groupId>com.example</groupId>
  <artifactId>Infra</artifactId>
  <scope>test</scope>
</dependency>
```

`src/main` versus `src/test` is about who can see the code, not what it is for.
Everything a consumer references lives in `Infra/src/main`; "only for testing"
is expressed by that `<scope>test</scope>` at the consumer, so no docker code
reaches a production classpath. `Infra/src/test` holds only Infra's own tests —
which double as the worked examples a consumer copies.

Editing a compose file in `src/main/resources` and re-running takes effect:
Maven copies it to `target/classes` and `DockerAssets` copies it on from there,
overwriting. To use your own copies instead, and stop it extracting at all:

```
-Dinfra.docker.dir=<path>
```

### Production takes the same path

`Infra` never appears in a production run. The difference is one line, because
both sides build the same config object:

```java
ClusterConfig cluster = ClusterConfig.load();                                    // prod: external file
ClusterConfig cluster = ClusterConfig.of(hadoop.clusterProperties(), "docker");  // test: from the container
```

Everything downstream — the Spark session, the data store — is identical, so the
tests exercise the real configuration mapping rather than a test-only shortcut.

## The development loop

Extend the module's `SparkTestSupport` and the session, fixture builders and
assertions are all in scope unqualified:

```java
public class NormalizeNameTest extends SparkTestSupport {

    private static Transformer normalizeName() {
        return input -> input.withColumn("name", lower(trim(col("name"))));
    }

    @Test
    public void trimsAndLowercases() {
        Dataset<Row> input = df("id INT, name STRING",
                row(1, "  Ada "),
                row(2, "GRACE"));

        assertData(df("id INT, name STRING",
                row(1, "ada"),
                row(2, "grace")), normalizeName().transform(input));
    }
}
```

1. Write the test, with the candidate step defined **inside the test** while its
   behaviour is in flux.
2. Iterate until the behaviour has settled.
3. Promote the step to a named class in `src/main/java`. The test should keep
   passing unchanged — that is the signal the move was clean.

`ModelDevelopmentExampleTest` is a worked example, and covers the four questions
worth asking of any step: the obvious case, schema stability, null handling, and
empty input. `ExtractorTest` and `LoaderTest` show the same loop for a real
file round-trip.

### `SparkTestSupport`

Each module carries **its own copy** in its own test tree, so the modules
stay independent — none needs another to compile or test. Keep the copies in
step when you change one.

The one crossing is deliberate: `DataStoreContract` extends `IO`'s copy, so a
backend test extending the contract inherits that one and shares its session.
The backend's own copy stays for its own tests.

| Helper | Purpose |
| --- | --- |
| `spark()` | the shared session |
| `df(ddl, rows...)`, `emptyDf(ddl)`, `row(values...)` | fixtures from a DDL schema |
| `tempDir(prefix)` | a fresh temp directory, for tests needing a real path |
| `assertData`, `assertDataInOrder` | row comparison with readable diffs |
| `assertSchema`, `assertColumns`, `assertColumnValues`, `assertRowCount` | narrower assertions |

Two deliberate choices:

- **The session is JVM-wide, not per-class.** Starting Spark costs seconds;
  reusing it means a module pays that once — the second test class in a module
  runs in well under a second. Tests must never stop the session.
- **`assertData` ignores row order.** Spark makes no ordering promise unless you
  ask for one, so an order-sensitive assertion is flaky by construction. Reach
  for `assertDataInOrder` only after an explicit `orderBy`.

The session redirects its warehouse into a unique temp directory, so test runs
leave no `spark-warehouse/`, `metastore_db/` or `derby.log` behind and
concurrent runs cannot collide.

## Build

Maven, with `JAVA_HOME` on Java 8.

```powershell
mvn clean package                 # everything
mvn test                          # tests only
mvn -pl OntologyTransform test    # one stage module (they have no interdependencies)
mvn -pl Hadood -am test           # a module plus what it depends on (IO, once it does)
```

Output: `<module>/target/<module>-1.0-SNAPSHOT.jar`

This machine has JDKs 8, 11 and 17 under `C:\java\`; the build targets Java 8
and `JAVA_HOME` already points there, so nothing needs setting. Running the
tests on Java 8 also sidesteps the `--add-opens` flags Spark 3.5 requires on
Java 17.

## Conventions

- A stage's module owns its own concerns: sources in `OntologyExtract`, domain
  shaping in `OntologyTransform`, sinks in `OntologyLoad`. Do not introduce a
  dependency between them.
- A backend implements the `IO` contracts, in the module that owns its client
  library. `IO` never depends on a backend — that would put every backend on
  every classpath — and a new backend proves itself by passing
  `DataStoreContract`, not by writing its own version of it.
- Test-only helpers live in the module's own test tree.
- Declare a new dependency's version in the parent's `<dependencyManagement>`
  and reference it version-less from the module that uses it.
- Adding a module: create the directory with a `pom.xml` pointing at the
  `OntologyProcessor` parent, and list it in the parent's `<modules>`.
- Nothing about a live system is compiled in. Hostnames, ports, paths and
  credentials come from the external file `Hadood/conf/cluster.properties.example`
  describes.
- Keep `spark.version` and `scala.version` in the parent `pom.xml` in sync when
  upgrading.
