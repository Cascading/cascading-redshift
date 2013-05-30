package org.pingles.cascading.redshift;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

public class RedshiftSafeTextDelimited extends TextDelimited {
    public RedshiftSafeTextDelimited(Fields sinkFields, Compress compress, String fieldDelimiter, String fieldQuote) {
        super(sinkFields, compress, fieldDelimiter, fieldQuote);
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();

        ensureTupleValuesAreWithinValidCodepoints(tuple);

        super.sink(flowProcess, sinkCall);
    }

    private void ensureTupleValuesAreWithinValidCodepoints(Tuple tuple) {
        for (Object value : tuple) {
            if (value != null) {
                String valueString = value.toString();

                for (int i = 0; i < valueString.length(); i++) {
                    if (isExcludedCodepoint(valueString.codePointAt(i))) {
                        throw new InvalidCodepointForRedshiftException(valueString);
                    }
                }
            }
        }
    }

    private boolean isExcludedCodepoint(int codepoint) {
        if (codepoint >= 0xD800 && codepoint <= 0xDFFF) {
            return true;
        }
        if (codepoint >= 0xFDD0 && codepoint <= 0xFDEF) {
            return true;
        }
        if (codepoint >= 0xFFFE && codepoint <=  0xFFFF) {
            return true;
        }
        return false;
    }
}
