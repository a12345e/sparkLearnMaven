package org.example.operation.task;

import org.example.operation.task.Measure;
import org.example.operation.task.Operation;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class OperationEvent {
    final private Operation operation;
    final private long start;
    final private Map<Measure, Object> measures;
    private Long end;
    public OperationEvent(Operation operation){
        this.operation = operation;
        measures = new HashMap<>();
        this.start = System.currentTimeMillis();
    }

    public Operation getOperation() {
        return operation;
    }

    public Map<Measure, Object> getMeasures() {
        return measures;
    }
    public void setMeasure(Measure measure, Object value) {
        measures.put(measure, value);
    }

    public long getStart() {
        return start;
    }
    public Long getEnd() {
        return end;
    }
    public void setEnd(Long value) {
        this.end = value;
    }
}
