package io.github.danilopiazza.aws.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

public class ObjectStorageClient {
    private final ObjectRequestProvider requestProvider;
    private final S3Client s3;
    private final String bucket;

    public ObjectStorageClient(S3Client s3, String bucket) {
        this(new ObjectRequestProvider(), s3, bucket);
    }

    ObjectStorageClient(ObjectRequestProvider requestProvider, S3Client s3, String bucket) {
        this.requestProvider = requestProvider;
        this.s3 = s3;
        this.bucket = bucket;
    }

    public byte[] getObject(String key) {
        return s3.getObjectAsBytes(requestProvider.getObjectRequest(bucket, key)).asByteArray();
    }

    public void putObject(String key, byte[] content) {
        s3.putObject(requestProvider.putObjectRequest(bucket, key), RequestBody.fromBytes(content));
    }
}
