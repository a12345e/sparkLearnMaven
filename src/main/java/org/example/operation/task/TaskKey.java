package org.example.operation.task;

import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.example.source.Source;

import java.io.Serializable;
import java.util.Optional;

public class TaskKey implements Serializable
{
    private final Environment environment;
    private final Mission mission;
    private final Owner owner;
    private final Optional<String> missingSpecific;
    private final Source source;
    private final String partitionName;
    private final String key;
    TaskKey(Environment environment,
            Mission mission,
            Owner owner,
            Source source,
            String partitionName,
            Optional<String> missionSpecific){
        this.environment=environment;
        this.mission=mission;
        this.owner=owner;
        this.missingSpecific=missionSpecific;
        this.source=source;
        this.partitionName = partitionName;
        String[] array = {environment.name(),mission.name(),owner.name(),source.name(),partitionName};
        String key = String.join("-", array);
        if (missingSpecific.isPresent()){
            key = key+"-"+missingSpecific.get();
        }
        this.key = key;
    }
    public Environment getEnvironment() {
        return environment;
    }

    public Mission getMission() {
        return mission;
    }

    public Owner getOwner() {
        return owner;
    }
    public Optional<String> getMissingSpecific() {
        return missingSpecific;
    }
    public Source getSource() {
        return source;
    }
    public String getPartitionName() {
        return partitionName;
    }
    @Override
    public String toString() {
        return key;
    }
}
