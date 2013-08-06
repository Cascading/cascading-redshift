package org.pingles.cascading.redshift;

import cascading.tap.SinkMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.Callable;

public class CommitTask implements Callable<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorTimeoutCommitter.class);

    private final RedshiftJdbcClient redshiftClient;
    private final S3Details s3Details;
    private final SinkMode sinkMode;
    private final RedshiftScheme scheme;

    public CommitTask(RedshiftConnectionDetails connectionDetails, S3Details s3Details, SinkMode sinkMode, RedshiftScheme scheme) {
        this.s3Details = s3Details;
        this.sinkMode = sinkMode;
        this.scheme = scheme;
        this.redshiftClient = new RedshiftJdbcClient(connectionDetails);
    }

    public Boolean call() throws Exception {
        try {
            redshiftClient.connect();

            if (sinkMode == SinkMode.REPLACE) {
                redshiftClient.execute(scheme.buildDropTableCommand());
                redshiftClient.execute(scheme.buildCreateTableCommand());
            }

            redshiftClient.execute(scheme.buildCopyFromS3Command(s3Details.getS3Uri(), s3Details.getAccessKey(), s3Details.getSecretKey()));
            redshiftClient.disconnect();

            LOGGER.info("Completed copy from S3");
        } catch (ClassNotFoundException e) {
            LOGGER.error("Couldn't commit to Redshift", e);
            return false;
        } catch (SQLException e) {
            LOGGER.error("Couldn't commit to Redshift", e);
            return false;
        }

        return true;
    }
}
