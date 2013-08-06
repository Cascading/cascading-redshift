package org.pingles.cascading.redshift;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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

    private static final Integer DEFAULT_TIMEOUT_MINUTES = 10;
    private Integer minutesToWait = DEFAULT_TIMEOUT_MINUTES;

    /**
     * Redshift sink-only tap to stage data to S3 and then issue a JDBC COPY command to specified Redshift table.
     * Drops existing table by default.
     *
     * To choose {@link SinkMode}, use {@link #RedshiftTap(String, String, String, String, String, String, RedshiftScheme, cascading.tap.SinkMode)}
     *
     * @param s3Uri uri of s3 staging directory
     * @param s3AccessKey aws access key
     * @param s3SecretKey aws secret key
     * @param jdbcUrl Redshift jdbc url
     * @param username Redshift user name
     * @param password Redshift password
     * @param scheme scheme object
     */
    public RedshiftTap(String s3Uri, String s3AccessKey, String s3SecretKey, String jdbcUrl, String username, String password, RedshiftScheme scheme) {
        this(s3Uri, s3AccessKey, s3SecretKey, jdbcUrl, username, password, scheme, SinkMode.REPLACE, DEFAULT_TIMEOUT_MINUTES);
    }

    /**
     * Redshift sink-only tap to stage data to S3 and then issue a JDBC COPY command to specified Redshift table
     *
     * @param s3Uri uri of s3 staging directory
     * @param s3AccessKey aws access key
     * @param s3SecretKey aws secret key
     * @param jdbcUrl Redshift jdbc url
     * @param username Redshift user name
     * @param password Redshift password
     * @param scheme scheme object
     * @param sinkMode use {@link SinkMode#REPLACE} to drop Redshift table before loading;
     *                 {@link SinkMode#UPDATE} to not drop table for incremental loading
     */
    public RedshiftTap(String s3Uri, String s3AccessKey, String s3SecretKey, String jdbcUrl, String username, String password, RedshiftScheme scheme, SinkMode sinkMode) {
        this(s3Uri, s3AccessKey, s3SecretKey, jdbcUrl, username, password, scheme, sinkMode, DEFAULT_TIMEOUT_MINUTES);
    }


    /**
     * Redshift sink-only tap to stage data to S3 and then issue a JDBC COPY command to specified Redshift table
     *
     * @param s3Uri uri of s3 staging directory
     * @param s3AccessKey aws access key
     * @param s3SecretKey aws secret key
     * @param jdbcUrl Redshift jdbc url
     * @param username Redshift user name
     * @param password Redshift password
     * @param scheme scheme object
     * @param sinkMode use {@link SinkMode#REPLACE} to drop Redshift table before loading;
     *                 {@link SinkMode#UPDATE} to not drop table for incremental loading
     * @param minutesToWait The Redshift COPY statement can take a long time to execute. Specifying a timeout here will
     *                       disconnect the client after the period of time specified- the COPY statement will have been
     *                       submitted and will continue to execute.
     */
    public RedshiftTap(String s3Uri, String s3AccessKey, String s3SecretKey, String jdbcUrl, String username, String password, RedshiftScheme scheme, SinkMode sinkMode, Integer minutesToWait) {
        super(new RedshiftSafeTextDelimited(scheme.getSinkFields(), TextLine.Compress.ENABLE, scheme.getFieldDelimiter()), s3Uri, sinkMode);
        this.s3Uri = s3Uri;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.scheme = scheme;
        this.minutesToWait = minutesToWait;
    }



    @Override
    public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        conf.set("mapred.output.compression.type", "BLOCK");
        super.sinkConfInit(process, conf);
    }

    @Override
    public boolean commitResource(JobConf conf) throws IOException {
        RedshiftConnectionDetails connDetails = new RedshiftConnectionDetails(jdbcUrl, username, password);
        S3Details s3Details = new S3Details(s3Uri, s3AccessKey, s3SecretKey);
        CommitTask task = new CommitTask(connDetails, s3Details, getSinkMode(), scheme);
        ResourceCommitter committer = new ExecutorTimeoutCommitter(task, new Timeout(minutesToWait, TimeUnit.MINUTES));
        return committer.commit();
    }

    @Override
    public String getIdentifier() {
        return getJDBCPath() + this.id;
    }

    public String getJDBCPath() {
        return "jdbc:/" + jdbcUrl.replaceAll( ":", "_" );
    }
}
