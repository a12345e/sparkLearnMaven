package org.example.source;

public enum Supplier {

    FICTIVE_ASUP("a@a.com", "1000000"),
    FICTIVE_BSUP("b@b.com", "1000000");

    Supplier(String email, String phone){
        this.email = email;
        this.phone = phone;
    }
    final String email;
    final String phone;
}
