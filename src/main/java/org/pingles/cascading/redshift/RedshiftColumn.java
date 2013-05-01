package org.pingles.cascading.redshift;

public class RedshiftColumn {
    private final String name;
    private final String definition;

    // Name: column name
    // Definition: VARCHAR(100) NULLABLE etc.
    public RedshiftColumn(String name, String definition) {
        this.name = name;
        this.definition = definition;
    }
}
