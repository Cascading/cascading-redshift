package org.pingles.cascading.redshift;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.nio.charset.Charset;

public class RedshiftSafeTextDelimited extends TextDelimited {
    private final RedshiftSafeDelimitedParser safeDelimitedParser;
    private final boolean skipHeader;
    private final boolean safe;
    private final boolean strict;
    private final Class[] types;

    public RedshiftSafeTextDelimited(Fields sinkFields, Compress compress, String fieldDelimiter) {
        super(sinkFields, compress, fieldDelimiter);
        types = null;
        strict = false;
        safe = false;
        skipHeader = false;

        safeDelimitedParser = new RedshiftSafeDelimitedParser(fieldDelimiter, RedshiftSafeDelimitedParser.REDSHIFT_FIELD_QUOTE, types, strict, safe, skipHeader, getSourceFields(), getSinkFields());
    }

    @Override
    public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Object[] context = new Object[] {new Text(), new StringBuilder(4 * 1024), Charset.forName(DEFAULT_CHARSET)};
        sinkCall.setContext(context);
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();
        Text text = (Text) sinkCall.getContext()[0];
        StringBuilder line = (StringBuilder) sinkCall.getContext()[1];
        Charset charset = (Charset) sinkCall.getContext()[2];

        line = (StringBuilder) safeDelimitedParser.joinLine(tuple, line);
        text.set(line.toString().getBytes(charset));

        sinkCall.getOutput().collect(null, text);

        line.setLength(0);
    }
}
