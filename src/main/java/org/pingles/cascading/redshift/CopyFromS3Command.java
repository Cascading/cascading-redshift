package org.pingles.cascading.redshift;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CopyFromS3Command implements RedshiftJdbcCommand {
    private final String tableName;
    private final String uri;
    private final String s3AccessKey;
    private final String s3SecretKey;

    public CopyFromS3Command(String tableName, String uri, String s3AccessKey, String s3SecretKey) {
        this.tableName = tableName;
        this.uri = uri;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
    }

    public void execute(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(commandStatement());
    }

    private String commandStatement() {
        return String.format("COPY %s from '%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' delimiter '\\t';", tableName, convertToAmazonUri(uri), s3AccessKey, s3SecretKey);
    }

    private String convertToAmazonUri(String hfsS3Path) {
        return hfsS3Path.replaceFirst("s3n", "s3");
    }

    @Override
    public String toString() {
        return commandStatement();
    }
}
