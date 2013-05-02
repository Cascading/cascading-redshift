package org.pingles.cascading.redshift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateTableCommand implements RedshiftJdbcCommand {
    private final String tableName;
    private final String[] columnNames;
    private final String[] columnDefinitions;
    private final String distributionKey;
    private final String[] sortKeys;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftJdbcClient.class);

    public CreateTableCommand(String tableName, String[] columnNames, String[] columnDefinitions, String distributionKey, String[] sortKeys) {
        if (columnNames.length != columnDefinitions.length) {
            throw new IllegalArgumentException("Column names and definitions must have the same length");
        }
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnDefinitions = columnDefinitions;
        this.distributionKey = distributionKey;
        this.sortKeys = sortKeys;
    }

    public void execute(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(queryString());
    }

    private String queryString() {
        StringBuffer sortKeyBuffer = new StringBuffer();
        for (String sortKey : sortKeys) {
            if (sortKeyBuffer.length() > 0) {
                sortKeyBuffer.append(", ");
            }
            sortKeyBuffer.append(sortKey);
        }

        return String.format("CREATE TABLE %s (%s) DISTKEY (%s) SORTKEY (%s)", tableName, buildColumnDefinitionString(), distributionKey, sortKeyBuffer.toString());
    }

    private String buildColumnDefinitionString() {
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < columnNames.length; i++) {
            String colName = columnNames[i];
            String colDef = columnDefinitions[i];
            sb.append(colName + " " + colDef);

            if (i != columnNames.length - 1) {
                sb.append(", ");
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return queryString();
    }
}
