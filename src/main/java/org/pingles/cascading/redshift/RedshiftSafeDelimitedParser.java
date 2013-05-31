package org.pingles.cascading.redshift;

import cascading.scheme.util.DelimitedParser;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class RedshiftSafeDelimitedParser extends DelimitedParser {
    private static final char BACKSLASH = 0x5c;
    public static String REDSHIFT_FIELD_QUOTE = "\"";

    private final String delimiter;
    private final String quote;

    public RedshiftSafeDelimitedParser(String delimiter, String quote, Class[] types, boolean strict, boolean safe, boolean skipHeader, Fields sourceFields, Fields sinkFields) {
        super(delimiter, quote, types, strict, safe, skipHeader, sourceFields, sinkFields);
        this.delimiter = delimiter;
        this.quote = quote;
    }

    // i know this is horrible but we'll use this just for writing some tests
    public RedshiftSafeDelimitedParser(String delimiter, String quote) {
        this(delimiter, quote, null, true, true, true, Fields.ALL, Fields.ALL);
    }

    @Override
    public Appendable joinLine(Iterable iterable, Appendable buffer) {
        try {
            return joinWithQuote(iterable, buffer);
        } catch (IOException e) {
            throw new TapException("unable to append data", e);
        }
    }

    private Appendable joinWithQuote(Iterable tuple, Appendable buffer) throws IOException {
        int count = 0;

        for( Object value : tuple )
        {
            if (count != 0)
                buffer.append(delimiter);

            if (value != null)
            {
                if (value instanceof String) {
                    String valueString = value.toString();

                    if (containsAnyInvalidCodepoints(valueString)) {
                        throw new InvalidCodepointForRedshiftException(valueString);
                    }

                    String escaped = StringUtils.escapeString(valueString, BACKSLASH, new char[]{'"', '\''});
                    escaped = escaped.replaceAll("\n", "");
                    buffer.append(quote + escaped + quote);
                } else {
                    buffer.append(value.toString());
                }
            }

            count++;
        }

        return buffer;
    }

    private boolean containsAnyInvalidCodepoints(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (isExcludedCodepoint(s.codePointAt(i))) {
                return true;
            }
        }
        return false;
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
