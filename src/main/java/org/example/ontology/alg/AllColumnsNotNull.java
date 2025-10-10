package org.example.ontology.alg;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class AllColumnsNotNull {
    public static Column valid(Set<String> colNames) {
        if (colNames == null || colNames.isEmpty()) {
            // Return a "true" literal if no columns given
            return lit(true);
        }
        for (String col: colNames){
            assert col != null && !col.isEmpty();
        }
        List<Column> cols = colNames.stream().map(functions::col).collect(Collectors.toList());
        // Start with first column condition
        Column condition = cols.get(0).isNotNull();

        // Chain .and() for the rest
        for (int i = 1; i < cols.size(); i++) {
            condition = condition.and(cols.get(i).isNotNull());
        }

        return condition;
    }
}
