package io.github.danilopiazza.aws.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@ExtendWith(MockitoExtension.class)
class ObjectStorageClientTest {
    static final String BUCKET = "test-bucket";
    static final String KEY = "test-key";
    static final byte[] CONTENT = "test-content".getBytes(StandardCharsets.UTF_8);

    @Mock
    ObjectRequestProvider requestProvider;
    @Mock
    S3Client s3;
    @Mock
    Consumer<GetObjectRequest.Builder> getObjectRequestConsumer;
    @Mock
    Consumer<PutObjectRequest.Builder> putObjectRequestConsumer;

    ObjectStorageClient client;

    @BeforeEach
    void init() {
        client = new ObjectStorageClient(requestProvider, s3, BUCKET);
    }

    @Test
    void testGetObject() {
        var getObjectResponse = GetObjectResponse.builder().build();
        var responseBytes = ResponseBytes.fromByteArray(getObjectResponse, CONTENT);
        when(requestProvider.getObjectRequest(BUCKET, KEY)).thenReturn(getObjectRequestConsumer);
        when(s3.getObjectAsBytes(getObjectRequestConsumer)).thenReturn(responseBytes);

        assertThat(client.getObject(KEY), is(equalTo(CONTENT)));
    }

    @Test
    void testPutObject() {
        try (var mockedRequestBody = mockStatic(RequestBody.class)) {
            var requestBody = RequestBody.empty();
            mockedRequestBody.when(() -> RequestBody.fromBytes(CONTENT)).thenReturn(requestBody);
            when(requestProvider.putObjectRequest(BUCKET, KEY)).thenReturn(putObjectRequestConsumer);

            client.putObject(KEY, CONTENT);

            verify(s3).putObject(putObjectRequestConsumer, requestBody);
        }
    }
}
