package org.pingles.cascading.redshift;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CopyFromS3Command implements RedshiftJdbcCommand {
    private final String tableName;
    private final String uri;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String[] copyOptions;

    public CopyFromS3Command(String tableName, String uri, String s3AccessKey, String s3SecretKey, String[] copyOptions) {
        this.tableName = tableName;
        this.uri = uri;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.copyOptions = copyOptions;
    }

    public void execute(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(commandStatement());
    }

    private String commandStatement() {
        return String.format("COPY %s from '%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' delimiter '\\t' %s;", tableName, convertToAmazonUri(uri), s3AccessKey, s3SecretKey, buildCopyOptions());
    }

    private String buildCopyOptions() {
        StringBuffer buffer = new StringBuffer();

        for (String copyOption : copyOptions) {
            if (buffer.length() > 0) {
                buffer.append(" ");
            }
            buffer.append(copyOption);
        }

        return buffer.toString();
    }

    private String convertToAmazonUri(String hfsS3Path) {
        return hfsS3Path.replaceFirst("s3n", "s3");
    }

    @Override
    public String toString() {
        return commandStatement();
    }
}
