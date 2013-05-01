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
        System.out.println("Executing: " + command.toString());
        command.execute(connection);
    }

    public void connect() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
//        LOGGER.info("Connecting to {} authenticating with {} {}", new Object[] {jdbcUrl, username, password});

        System.out.println("Connecting to " + jdbcUrl + " authenticating with " + username + " " + password);

        connection = DriverManager.getConnection(jdbcUrl, username, password);
    }
}
