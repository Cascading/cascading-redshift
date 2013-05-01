package org.pingles.cascading.redshift;

import java.sql.Connection;
import java.sql.SQLException;

public interface RedshiftJdbcCommand {
    void execute(Connection connection) throws SQLException;
}
