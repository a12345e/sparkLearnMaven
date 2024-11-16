package com.example;

import java.io.Serializable;

public class PersonAlias implements Serializable {
    private int id;
    private String alias;

    public PersonAlias(int id, String alias) {
        this.id = id;
        this.alias = alias;
    }

    // Getters and setters
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        return "PersonAlias{" +
                "id=" + id +
                ", alias='" + alias + '\'' +
                '}';
    }
}