package io.github.danilopiazza.aws.s3;

import java.util.function.Consumer;

import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class ObjectRequestProvider {
    Consumer<GetObjectRequest.Builder> getObjectRequest(String bucket, String key) {
        return b -> b.bucket(bucket).key(key);
    }

    Consumer<PutObjectRequest.Builder> putObjectRequest(String bucket, String key) {
        return b -> b.bucket(bucket).key(key);
    }
}
