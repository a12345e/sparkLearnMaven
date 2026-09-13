package infra.datasource;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * The Spark shape of an ontology data source.
 *
 * <p>This is the one place the Spark side of a definition is described. The
 * classes themselves - {@link OntologyDataSource}, {@link OntologyDataSourceKey},
 * {@link OntologyDataSourceExtract} - stay plain Java holders that know nothing
 * about Spark, so they can be used, tested and read as JSON without a Spark
 * class anywhere near them. Everything Spark needs lives here instead.
 *
 * <p>Read a file of definitions with it:
 *
 * <pre>
 * Dataset&lt;Row&gt; sources = spark.read()
 *         .schema(OntologyDataSourceSchema.dataSource())
 *         .json(path);
 * </pre>
 *
 * <p>Handing Spark the schema rather than letting it infer one means a
 * definitions file reads the same way every time: an empty file still has
 * columns, and a file that happens to be missing a field does not quietly
 * produce a narrower frame than the one downstream code expects.
 *
 * <p>A StructType is immutable, so each is built once and shared.
 */
public final class OntologyDataSourceSchema {

    /**
     * The shape of a {@link OntologyDataSourceKey}. Field order and names match
     * the class's fields, so a Row read with this lines up with its constructor.
     */
    private static final StructType KEY = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("env", DataTypes.StringType, true),
            // Nullable even though the field is a primitive int: this describes
            // the data on disk, where an id can simply be absent.
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("product", DataTypes.StringType, true),
            DataTypes.createStructField("mission", DataTypes.StringType, true)
    });

    /**
     * The shape of an {@link OntologyDataSourceExtract}. Its {@code HIVE} field
     * is deliberately absent, being a constant rather than part of a definition.
     */
    private static final StructType EXTRACT = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("tableName", DataTypes.StringType, true),
            DataTypes.createStructField("technology", DataTypes.StringType, true),
            DataTypes.createStructField("sql", DataTypes.StringType, true)
    });

    /**
     * The shape of a whole {@link OntologyDataSource}, nesting the two above.
     *
     * <p>Every field is nullable. These describe definitions as they sit in a
     * file, where any field may simply be absent - which matches
     * {@link OntologyDataSource#fromJson(String)}, equally tolerant of a
     * missing field.
     */
    private static final StructType DATA_SOURCE = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("key", KEY, true),
            DataTypes.createStructField("site", DataTypes.StringType, true),
            DataTypes.createStructField("extract", EXTRACT, true),
            DataTypes.createStructField("transform", DataTypes.StringType, true),
            DataTypes.createStructField("minimal_dt", DataTypes.StringType, true),
            DataTypes.createStructField("maximal_dt", DataTypes.StringType, true),
            // The enum travels as its name, the same text fromJson reads.
            DataTypes.createStructField("status", DataTypes.StringType, true)
    });

    /** The shape of a whole data source. */
    public static StructType dataSource() {
        return DATA_SOURCE;
    }

    /** The shape of a data source key, as nested in {@link #dataSource()}. */
    public static StructType key() {
        return KEY;
    }

    /** The shape of a data source extract, as nested in {@link #dataSource()}. */
    public static StructType extract() {
        return EXTRACT;
    }

    /** A description, not a thing to hold: there is nothing to instantiate. */
    private OntologyDataSourceSchema() {
    }

}
