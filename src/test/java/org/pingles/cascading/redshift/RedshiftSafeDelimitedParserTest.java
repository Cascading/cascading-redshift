package org.pingles.cascading.redshift;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class RedshiftSafeDelimitedParserTest {
    @Test
    public void shouldJoinValuesAndQuoteStringField() {
        RedshiftSafeDelimitedParser parser = new RedshiftSafeDelimitedParser(",", "\"");
        StringBuffer buf = new StringBuffer();

        parser.joinLine(new Tuple("Hello", "world"), buf);

        assertEquals("\"Hello\",\"world\"", buf.toString());
    }

    @Test
    public void shouldJoinValuesWithoutQuotingNumeric() {
        RedshiftSafeDelimitedParser parser = new RedshiftSafeDelimitedParser(",", "\"");
        StringBuffer buf = new StringBuffer();

        parser.joinLine(new Tuple("Hello", 102), buf);

        assertEquals("\"Hello\",102", buf.toString());
    }

    @Test
    public void shouldEscapeSingleQuotes() {
        RedshiftSafeDelimitedParser parser = new RedshiftSafeDelimitedParser(",", "\"");
        StringBuffer buf = new StringBuffer();

        parser.joinLine(new Tuple("Some", "'name"), buf);

        assertEquals("\"Some\",\"\\'name\"", buf.toString());
    }

    @Test
    public void shouldThrowErrorWithInvalidCodepointCharacter() throws UnsupportedEncodingException {
        RedshiftSafeDelimitedParser parser = new RedshiftSafeDelimitedParser(",", "\"");
        StringBuffer buf = new StringBuffer();

        byte[] characterBytes = new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80};

        try {
            parser.joinLine(new Tuple(new String(characterBytes, "UTF-8")), buf);
            fail("Didn't throw InvalidCodepointForRedshiftException");
        } catch (InvalidCodepointForRedshiftException e) {
            // success!
        }
    }
}
