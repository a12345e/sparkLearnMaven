package org.example.source;

import static org.example.source.Supplier.FICTIVE_ASUP;
import static org.example.source.Supplier.FICTIVE_BSUP;

public enum Source {

    FICTIVE_A(FICTIVE_ASUP),
    FICTIVE_B(FICTIVE_BSUP);

    final private Supplier supplier;
    Source(Supplier supplier){
        this.supplier = supplier;
    }

    public Supplier getSupplier() {
        return supplier;
    }
}
