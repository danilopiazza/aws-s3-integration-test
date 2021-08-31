package io.github.danilopiazza.aws.s3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;
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
        when(requestProvider.putObjectRequest(BUCKET, KEY)).thenReturn(putObjectRequestConsumer);

        client.putObject(KEY, CONTENT);

        verify(s3).putObject(argThat(is(putObjectRequestConsumer)), argThat(containsStream(readingBytes(CONTENT))));
    }

    static Matcher<RequestBody> containsStream(Matcher<InputStream> matcher) {
        return new TypeSafeMatcher<RequestBody>() {
            @Override
            protected boolean matchesSafely(RequestBody item) {
                return matcher.matches(item.contentStreamProvider().newStream());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a RequestBody containing ").appendDescriptionOf(matcher);
            }

            @Override
            protected void describeMismatchSafely(RequestBody item, Description mismatchDescription) {
                mismatchDescription.appendText("does not contain an InputStream ");
            }
        };
    }

    static Matcher<InputStream> readingBytes(Matcher<byte[]> matcher) {
        return new TypeSafeDiagnosingMatcher<InputStream>() {
            @Override
            protected boolean matchesSafely(InputStream item, Description mismatchDescription) {
                try {
                    return matcher.matches(item.readAllBytes());
                } catch (IOException e) {
                    mismatchDescription.appendValue(e);
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("an InputStream reading ").appendDescriptionOf(matcher);
            }
        };
    }

    static Matcher<InputStream> readingBytes(byte[] bytes) {
        return readingBytes(equalTo(bytes));
    }
}
