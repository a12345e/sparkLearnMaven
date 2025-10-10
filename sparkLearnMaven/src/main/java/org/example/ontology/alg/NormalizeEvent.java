package org.example.ontology.alg;


import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.ontology.OntEntityAssign;
import org.example.ontology.OntEventAssign;
import org.example.ontology.OntPropertyAssign;
import org.example.ontology.OntRelationAssign;

import static org.apache.spark.sql.functions.*;

import java.util.Set;

public class NormalizeEvent {
    StringNormalizer stringNormalizer = new StringNormalizer();
    AnyNullColAllNull anyNullColAllNull = new AnyNullColAllNull();
    AnyColumnIsNotNullCondition anyColumnIsNotNull = new AnyColumnIsNotNullCondition();

    private Column isValidAfterNormalizationCondition(OntRelationAssign relation){
        Column valid =   isValidAfterNormalizationCondition(relation.getTo());
        for (OntEntityAssign entity: relation.getFrom()){
            valid = valid.and(isValidAfterNormalizationCondition(entity));
        }
        return valid;

    }
    private Column isValidAfterNormalizationCondition(OntEntityAssign entity){
        return AllColumnsNotNull.valid(entity.getMandatoryColumnNames());
    }
    private Dataset<Row> normalize(Dataset<Row> data, Set<OntPropertyAssign> properties){
        for(OntPropertyAssign p: properties){
            data = stringNormalizer.normalize(data,p);
            data = p.getType().getPropertyNormalizer().normalize(data, p);
        }
        return data;
    }
    private Dataset<Row> normalize(Dataset<Row> data, OntEntityAssign entity){
        data = normalize(data, entity.getOptionalPropertiesAssignments());
        data = normalize(data, entity.getMandatoryPropertiesAssignments());
        data = anyNullColAllNull.transform(data, entity.getMandatoryColumnNames());
        return data;
    }
    private Dataset<Row> normalize(Dataset<Row> data, OntRelationAssign relation){
        data = normalize(data, relation.getProperties());
        data = normalize(data,relation.getTo());
        for(OntEntityAssign entity: relation.getFrom()){
            data = normalize(data,entity);
        }
        return data;
    }
    public Dataset<Row> normalize(Dataset<Row> data, OntEventAssign event){
        data = normalize(data, event.getProperties());
        for(OntRelationAssign relationAssign: event.getRelations()){
            data = normalize(data, relationAssign);
        }
        Column thereIsValidRelationAfterNormalization = lit(false);
        for(OntRelationAssign relation: event.getRelations()){
            thereIsValidRelationAfterNormalization = thereIsValidRelationAfterNormalization.
                    or(isValidAfterNormalizationCondition(relation));
        }
        data = data.filter(thereIsValidRelationAfterNormalization);
        return data;
    }
}
