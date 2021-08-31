package io.github.danilopiazza.aws.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

@Testcontainers
class ObjectStorageClientIT {
    static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack");
    static final String BUCKET = "test-bucket";

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(LOCALSTACK_IMAGE).withServices(S3);

    S3Client s3;
    ObjectStorageClient client;

    @BeforeEach
    void init() {
        s3 = S3Client.builder().endpointOverride(localstack.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider
                        .create(AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .region(Region.of(localstack.getRegion())).build();
        s3.createBucket(b -> b.bucket(BUCKET));
        client = new ObjectStorageClient(s3, BUCKET);
    }

    @Nested
    class ClientTest {
        String key;
        byte[] content;

        @BeforeEach
        void init() {
            key = UUID.randomUUID().toString();
            content = UUID.randomUUID().toString().getBytes();
        }

        @Test
        void givenNonExistingKeyWhenGetObjectThenThrows() {
            assertThrows(NoSuchKeyException.class, () -> client.getObject(key));
        }

        @Test
        void givenKeyExistsWhenGetObjectThenReturnsContent() {
            s3.putObject(b -> b.bucket(BUCKET).key(key), RequestBody.fromBytes(content));

            assertArrayEquals(content, client.getObject(key));
        }

        @Test
        void whenPutObjectThenWritesContent() throws Exception {
            client.putObject(key, content);

            assertArrayEquals(content, s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray());
        }
    }
}
