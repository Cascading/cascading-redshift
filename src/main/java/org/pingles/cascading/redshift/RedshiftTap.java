package org.pingles.cascading.redshift;

import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

public class RedshiftTap extends Hfs {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftJdbcClient.class);

    private final String username;
    private final String password;
    private final RedshiftScheme scheme;
    private final String s3Uri;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String jdbcUrl;
    private final String id = UUID.randomUUID().toString();

    public RedshiftTap(String s3Uri, String s3AccessKey, String s3SecretKey, String jdbcUrl, String username, String password, RedshiftScheme scheme) {
        super(new TextDelimited(scheme.getSinkFields()), s3Uri, SinkMode.REPLACE);
        this.s3Uri = s3Uri;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.scheme = scheme;
    }

    @Override
    public boolean commitResource(JobConf conf) throws IOException {
        // trigger copy from S3 to Redshift
        RedshiftJdbcClient client = new RedshiftJdbcClient(jdbcUrl, username, password);
        try {
            client.connect();
            client.execute(scheme.buildDropTableCommand());
            client.execute(scheme.buildCreateTableCommand());
            client.execute(scheme.buildCopyFromS3Command(s3Uri, s3AccessKey, s3SecretKey));
        } catch (ClassNotFoundException e) {
            LOGGER.error("Couldn't commit to Redshift", e);
            return false;
        } catch (SQLException e) {
            LOGGER.error("Couldn't commit to Redshift", e);
            return false;
        }

        return true;
    }

    @Override
    public String getIdentifier() {
        return getJDBCPath() + this.id;
    }

    public String getJDBCPath() {
        return "jdbc:/" + jdbcUrl.replaceAll( ":", "_" );
    }
}
