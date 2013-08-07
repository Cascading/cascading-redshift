package org.pingles.cascading.redshift;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.util.List;

public class RedshiftSafeTextDelimited extends TextDelimited {
    private final RedshiftSafeDelimitedParser safeDelimitedParser;
    private final boolean skipHeader;
    private final boolean safe;
    private final boolean strict;
    private final Class[] types;
    private final boolean writeHeader;

    public RedshiftSafeTextDelimited(Fields sinkFields, Compress compress, String fieldDelimiter) {
        super(sinkFields, compress, fieldDelimiter);
        types = null;
        strict = false;
        safe = false;
        skipHeader = false;
        writeHeader = false;

        safeDelimitedParser = new RedshiftSafeDelimitedParser(fieldDelimiter, RedshiftSafeDelimitedParser.REDSHIFT_FIELD_QUOTE, types, strict, safe, skipHeader, getSourceFields(), getSinkFields());
    }

    @Override
    public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        sinkCall.setContext(new Object[]{new DecoratorTuple(), new StringBuilder(4 * 1024)});

        if( writeHeader )
        {
            Fields fields = sinkCall.getOutgoingEntry().getFields();
            StringBuilder line = (StringBuilder) safeDelimitedParser.joinLine( fields, (StringBuilder) sinkCall.getContext()[ 1 ] );

            DecoratorTuple decoratorTuple = (DecoratorTuple) sinkCall.getContext()[ 0 ];

            decoratorTuple.set( Tuple.NULL, line.toString() );

            line.setLength( 0 );

            sinkCall.getOutput().collect( null, decoratorTuple );
        }
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();
        StringBuilder line = (StringBuilder) safeDelimitedParser.joinLine( tuple, (StringBuilder) sinkCall.getContext()[ 1 ] );

        DecoratorTuple decoratorTuple = (DecoratorTuple) sinkCall.getContext()[ 0 ];

        decoratorTuple.set( tuple, line.toString() );

        line.setLength( 0 );

        sinkCall.getOutput().collect( null, decoratorTuple );
    }

    /**
     *  Taken from Cascading's TextDelimited
     */
    private static class DecoratorTuple extends Tuple
    {
        String string;

        private DecoratorTuple()
        {
            super( (List<Object>) null );
        }

        public void set( Tuple tuple, String string )
        {
            this.setAll(tuple);
            this.string = string;
        }

        @Override
        public String toString()
        {
            return string;
        }
    }
}
