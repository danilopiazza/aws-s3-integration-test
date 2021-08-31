package io.github.danilopiazza.aws.s3;

import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class ObjectRequestProviderTest {
    static final String BUCKET = "test-bucket";
    static final String KEY = "test-key";

    ObjectRequestProvider provider = new ObjectRequestProvider();

    @Test
    void testGetObjectRequest() {
        var builder = mock(GetObjectRequest.Builder.class, RETURNS_SELF);

        provider.getObjectRequest(BUCKET, KEY).accept(builder);

        verify(builder).bucket(BUCKET);
        verify(builder).key(KEY);
    }

    @Test
    void testPutObjectRequest() {
        var builder = mock(PutObjectRequest.Builder.class, RETURNS_SELF);

        provider.putObjectRequest(BUCKET, KEY).accept(builder);

        verify(builder).bucket(BUCKET);
        verify(builder).key(KEY);
    }
}
