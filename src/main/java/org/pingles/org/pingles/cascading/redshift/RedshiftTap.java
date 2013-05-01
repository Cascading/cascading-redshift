package org.pingles.org.pingles.cascading.redshift;

import cascading.flow.FlowProcess;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Dfs;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.twitter.maple.jdbc.JDBCScheme;
import com.twitter.maple.jdbc.JDBCTap;
import com.twitter.maple.jdbc.TableDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class RedshiftTap extends Tap<JobConf, RecordReader, OutputCollector> {
    private final RedshiftScheme scheme;
    private final JDBCTap jdbcTap;
    private final SinkMode sinkMode;

    public RedshiftTap(String s3Uri, String jdbcUrl, String username, String password, TableDesc tableDescription, RedshiftScheme scheme) {
        TextDelimited dfsScheme = new TextDelimited();
        sinkMode = SinkMode.REPLACE;
        Dfs dfs = new Dfs(dfsScheme, s3Uri, sinkMode);

        JDBCScheme jdbcScheme = scheme.buildJdbcScheme();

        this.jdbcTap = new JDBCTap(jdbcUrl, username, password, "org.postgresql.Driver", tableDescription, jdbcScheme, sinkMode);
        this.scheme = scheme;
    }

    @Override
    public String getIdentifier() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> jobConfFlowProcess, RecordReader recordReader) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> jobConfFlowProcess, OutputCollector outputCollector) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean createResource(JobConf entries) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean deleteResource(JobConf entries) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean resourceExists(JobConf entries) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getModifiedTime(JobConf entries) throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
