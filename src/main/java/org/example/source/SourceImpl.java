package org.example.source;

import org.example.ontology.OntEvent;

import java.util.Map;

public class SourceImpl {

    private final Supplier supplier;
    private final SourceInputDriver driver;
    private final SourcePreNormalizerTransformation preNormalizer;
    private final Map<SourceManagementProperty, Object> managementProperties;
    private final OntEvent event;
    public SourceImpl(Supplier supplier,
                      SourceInputDriver driver,
                      SourcePreNormalizerTransformation preNormalizer,
                      Map<SourceManagementProperty, Object> managementProperties,
                      OntEvent event){
        this.driver = driver;
        this.event = event;
        this.supplier = supplier;
        this.preNormalizer = preNormalizer;
        this.managementProperties = managementProperties;


    }

    public Supplier getSupplier() {
        return supplier;
    }

    public SourceInputDriver getDriver() {
        return driver;
    }

    public SourcePreNormalizerTransformation getPreNormalizer() {
        return preNormalizer;
    }

    public Map<SourceManagementProperty, Object> getManagementProperties() {
        return managementProperties;
    }

    public OntEvent getEvent() {
        return event;
    }
}
