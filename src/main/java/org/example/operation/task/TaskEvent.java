package org.example.operation.task;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TaskEvent {
    final private long start;
    private Long end;
    private  final Map<Operation, OperationEvent> operations;

    TaskEvent(){
        this.start = System.currentTimeMillis();
        this.operations = new HashMap<>();
    }

    public void set(Operation operation, OperationEvent operationEvent){
        operations.put(operation, operationEvent);
    }
    public long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }
}
