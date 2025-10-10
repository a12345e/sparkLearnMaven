package org.example.ontology;

import org.example.ontology.alg.NormalizeProperty;
import org.example.ontology.alg.StringNormalizer;
import org.example.ontology.alg.SupportsPropertyNormalizer;

public enum OntProperty implements SupportsPropertyNormalizer {
    wing(new StringNormalizer()),
    tail(new StringNormalizer()),
    color(new StringNormalizer()),
    manufacturer(new StringNormalizer()),
    start_time(new StringNormalizer()),
    time(new StringNormalizer()),
    end_time(new StringNormalizer());

    final NormalizeProperty normalizer;

    OntProperty(NormalizeProperty normalizer){
        this.normalizer = normalizer;
    }
    @Override
    public NormalizeProperty getPropertyNormalizer() {
        return normalizer;
    }
}
