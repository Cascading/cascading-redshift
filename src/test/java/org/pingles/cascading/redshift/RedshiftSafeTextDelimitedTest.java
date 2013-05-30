package org.pingles.cascading.redshift;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class RedshiftSafeTextDelimitedTest {
    @Test
    public void shouldNotRaiseExceptionWithAsciiString() throws IOException {
        RedshiftSafeTextDelimited scheme = createTextDelimitedScheme();

        Tuple tuple = new Tuple("Hello");

        try {
            scheme.sink(FlowProcess.NULL, new StubSinkCall(tuple));
        } catch (ClassCastException e) {
            // REEAALLLYYY disgusting but it's a massive pain with
            // cascading's api.
        }
    }

    @Test
    public void shouldThrowInvalidCodepointErrorWithExcludedCodepointString() throws IOException {
        // http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
        // 0xD800 - 0xDFFF
        // 0xFDD0 - 0xFDEF, 0xFFFE, and 0xFFFF

        RedshiftSafeTextDelimited scheme = createTextDelimitedScheme();

        // 0xD800
        byte[] characterBytes = new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80};
        Tuple tuple = new Tuple(new String(characterBytes));

        try {
            scheme.sink(FlowProcess.NULL, new StubSinkCall(tuple));
            fail("Should have raised an exception");
        } catch (InvalidCodepointForRedshiftException e) {
            // success
        } catch (ClassCastException e) {
            // mucky cascading
        }
    }

    private RedshiftSafeTextDelimited createTextDelimitedScheme() {
        return new RedshiftSafeTextDelimited(Fields.ALL, TextLine.Compress.DEFAULT, "", "");
    }

    private class StubSinkCall implements SinkCall<Object[], OutputCollector> {
        private final Tuple tuple;
        private final StringBuilder stringBuffer;

        private StubSinkCall(Tuple tuple) {
            this.tuple = tuple;
            this.stringBuffer = new StringBuilder();
        }

        public Object[] getContext() {
            return new Object[] {new Tuple(), stringBuffer};
        }

        public void setContext(Object[] objects) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public TupleEntry getOutgoingEntry() {
            return new TupleEntry(tuple);
        }

        public OutputCollector getOutput() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public String sunkString() {
            return stringBuffer.toString();
        }
    }
}
