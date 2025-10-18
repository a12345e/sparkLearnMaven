package org.example.source.drivers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.source.SourceConnectionParameter;
import org.example.source.SourceInputDriver;
import org.example.source.SourcePartitionLookupProperties;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HiveSourceInputDriver extends SourceInputDriverAbstract {

    public HiveSourceInputDriver(Map<SourceConnectionParameter, Object> params){
        super(params);
    }
    @Override
    public Dataset<Row> extract(Collection<String> partitions) {
        return null;
    }

    @Override
    public List<String> getPartitions(Map<SourcePartitionLookupProperties, Object> range) {
        return null;
    }
}
