package org.example.operation.task;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Task {
    private final TaskKey key;
    private TaskEvent event;
    private final List<TaskEvent> history;
    Task(TaskKey key){
        this.key = key;
        this.event = null;
        this.history = new ArrayList<>();
    }

    public TaskKey getKey() {
        return key;
    }

    public void addEvent(TaskEvent event) {
        if(this.event != null){
            history.add(event);
        }
        this.event = event;
    }
    public TaskEvent getEvent() {
        return event;
    }

    public List<TaskEvent> getHistory() {
        return history;
    }
    public static Task fromJson(String json) throws Exception {
        return new ObjectMapper().readValue(json, Task.class);
    }

    public String toJson() throws Exception {
        return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
}
