package org.pingles.org.pingles.cascading.redshift;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.twitter.maple.jdbc.JDBCScheme;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import java.io.IOException;

// Scheme<Config, Input, Output, SourceContext, SinkContext
public class RedshiftScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
    private final String[] columnNames;

    public RedshiftScheme(Fields sourceFields, Fields sinkFields, String[] columnNames) {
        super(sourceFields, sinkFields);
        this.columnNames = columnNames;
    }

    public RedshiftScheme(Fields sourceFields, Fields sinkFields, int numSinkParts, String[] columnNames) {
        super(sourceFields, sinkFields, numSinkParts);
        this.columnNames = columnNames;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> jobConfFlowProcess, Tap<JobConf, RecordReader, OutputCollector> jobConfRecordReaderOutputCollectorTap, JobConf entries) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> jobConfFlowProcess, Tap<JobConf, RecordReader, OutputCollector> jobConfRecordReaderOutputCollectorTap, JobConf entries) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean source(FlowProcess<JobConf> jobConfFlowProcess, SourceCall<Object[], RecordReader> recordReaderSourceCall) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void sink(FlowProcess<JobConf> jobConfFlowProcess, SinkCall<Object[], OutputCollector> outputCollectorSinkCall) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public JDBCScheme buildJdbcScheme() {
        return new JDBCScheme(getSinkFields(), columnNames, getNumSinkParts());
    }
}
