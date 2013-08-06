package org.pingles.cascading.redshift;

public class S3Details {
    private final String s3Uri;
    private final String accessKey;
    private final String secretKey;

    public S3Details(String s3Uri, String accessKey, String secretKey) {
        this.s3Uri = s3Uri;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public String getS3Uri() {
        return s3Uri;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }
}
