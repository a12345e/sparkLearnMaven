package org.example.source.drivers;

import org.example.source.SourceConnectionParameter;
import org.example.source.SourceInputDriver;

import java.util.Map;

public abstract class SourceInputDriverAbstract implements SourceInputDriver {
    private final Map<SourceConnectionParameter, Object> params;
    protected SourceInputDriverAbstract(Map<SourceConnectionParameter, Object> params){
        this.params = params;

    }

    public Map<SourceConnectionParameter, Object> getParams() {
        return params;
    }
}
