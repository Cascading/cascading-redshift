package org.pingles.cascading.redshift;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RedshiftJdbcClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftJdbcClient.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private Connection connection;

    public RedshiftJdbcClient(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public void execute(RedshiftJdbcCommand command) throws SQLException {
        LOGGER.info("execute {}", command.toString());
        command.execute(connection);
    }

    public void connect() throws ClassNotFoundException, SQLException {
        LOGGER.info("connecting to {} authenticating with {} {}", new Object[] {jdbcUrl, username, password});

        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(jdbcUrl, username, password);
    }
}
