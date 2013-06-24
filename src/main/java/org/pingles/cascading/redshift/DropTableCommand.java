package org.pingles.cascading.redshift;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DropTableCommand implements RedshiftJdbcCommand {
    private final String tableName;

    public DropTableCommand(String tableName) {
        this.tableName = tableName;
    }

    public void execute(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        try {
            statement.execute(commandStatement());
        } catch (SQLException e) {
            // ignore for now- most likely the table doesn't exist yet so can disregard
        }
    }

    private String commandStatement() {
        return String.format("DROP TABLE %s", tableName);
    }

    @Override
    public String toString() {
        return commandStatement();
    }
}
