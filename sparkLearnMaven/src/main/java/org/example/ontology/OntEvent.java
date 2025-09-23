package org.example.ontology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class OntEvent implements Normalize{

    private final List<OntProperty> properties;
    private final List<OntEntity> entities;
    private final List<OntRelation> relations;

    public List<OntProperty> getProperties(){
        return properties;
    }
    private  List<OntEntity> getEntities(){
        return entities;
    };
    private List<OntRelation> getRelations(){
        return relations;
    }
    public OntEvent(
            List<OntProperty> properties,
                    List<OntEntity> objects,
                    List<OntRelation> relations){
        this.properties = properties;
        this.entities = objects;
        this.relations = relations;
    }
    @Override
    public Dataset<Row> normalize(Dataset<Row> data){
        for(OntEntity entity: entities){
            data = entity.normalize(data);
        }
        for(OntRelation relation: relations){
            data = relation.normalize(data);
        }
        for(OntProperty p: properties){
            data = p.normalize(data);
        }
        return data;
    }

}
