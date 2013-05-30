package org.pingles.cascading.redshift;

public class InvalidCodepointForRedshiftException extends RuntimeException {
    private final String originalString;

    public InvalidCodepointForRedshiftException(String originalString) {
        this.originalString = originalString;
    }

    @Override
    public String getMessage() {
        return String.format("The string contains characters in the excluded range for Redshift. Original string: \"%s\"", originalString);
    }
}
