package org.example.source;



import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface SourceInputDriver {
    public Dataset<Row> extract(Collection<String> partitions);
    public List<String> getPartitions(Map<SourcePartitionLookupProperties,Object> range);

}
