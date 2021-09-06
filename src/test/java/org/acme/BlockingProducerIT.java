package org.acme;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.quarkus.test.bootstrap.RestService;
import io.quarkus.test.scenarios.QuarkusScenario;
import io.quarkus.test.services.QuarkusApplication;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.ext.web.client.predicate.ResponsePredicate;

@Tag("QUARKUS-1090")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@QuarkusScenario
public class BlockingProducerIT {
    private static final int TIMEOUT_SEC = 80;
    private static final int EVENTS = 3;
    static CustomStrimziKafkaContainer kafkaContainer;

    WebClient httpClient;

    @QuarkusApplication
    static RestService app = new RestService()
            .onPreStart(app -> {
                Map<String, String> kafkaProp = Map.of("auto.create.topics.enable", "false");
                kafkaContainer = new CustomStrimziKafkaContainer("0.24.0-kafka-2.7.0", kafkaProp);
                kafkaContainer.start();
            })
            .withProperty("kafka.bootstrap.servers", () -> kafkaContainer.getBootstrapServers());

    @BeforeEach
    public void setup() {
        httpClient = WebClient.create(Vertx.vertx(), new WebClientOptions());
        System.out.println("App Listening: " + app.getPort());
    }

    @AfterAll
    public void afterAll() {
        kafkaContainer.stop();
    }

    @Test
    public void kafkaClientsBlocksIfTopicsNotExistWithMetadata() throws InterruptedException {
        CountDownLatch done = new CountDownLatch(EVENTS);

        for (int i = 0; i < EVENTS; i++) {
            makeHttpReqAsJson(httpClient, "/event/tooLongToExist", HttpStatus.SC_OK).subscribe().with(success -> {
                assertEquals(success, "success");
                done.countDown();
            });
        }

        done.await(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertEquals(0, done.getCount(), String.format("Missing %d events.", EVENTS - done.getCount()));
    }

    @Test
    public void kafkaClientsBlocksIfTopicsNotExistEmitterWithoutMetadata() throws InterruptedException {
        CountDownLatch done = new CountDownLatch(EVENTS);

        for (int i = 0; i < EVENTS; i++) {
            makeHttpReqAsJson(httpClient, "/event", HttpStatus.SC_OK).subscribe().with(success -> {
                assertEquals(success, "success");
                done.countDown();
            });
        }

        done.await(TIMEOUT_SEC, TimeUnit.SECONDS);
        assertEquals(0, done.getCount(), String.format("Missing %d events.", EVENTS - done.getCount()));
    }

    private Uni<String> makeHttpReqAsJson(WebClient httpClient, String path, int expectedStatus) {
        return makeHttpReq(httpClient, path, expectedStatus).map(HttpResponse::bodyAsString);
    }

    private Uni<HttpResponse<Buffer>> makeHttpReq(WebClient httpClient, String path, int expectedStatus) {
        return httpClient.postAbs(getAppEndpoint() + path)
                .expect(ResponsePredicate.status(expectedStatus))
                .send();
    }

    private String getAppEndpoint() {
        return String.format("http://localhost:%d/", app.getPort());
    }
}
