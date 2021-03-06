package org.acme;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.quarkus.test.bootstrap.RestService;
import io.quarkus.test.scenarios.QuarkusScenario;
import io.quarkus.test.services.QuarkusApplication;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;

@Tag("QUARKUS-1090")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@QuarkusScenario
public class BlockingProducerIT {
    private static final int TIMEOUT_SEC = 5;
    private static final int EVENTS = 100;
    static final long KAFKA_MAX_BLOCK_MS = 100;
    static final long DEVIATION_ERROR_MS = 80;
    static final long KAFKA_MAX_BLOCK_TIME_MS = KAFKA_MAX_BLOCK_MS + DEVIATION_ERROR_MS;

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
    }

    @AfterAll
    public void afterAll() {
        kafkaContainer.stop();
    }

    @Test
    public void kafkaProducerBlocksIfTopicsNotExistWithMetadata() {
        UniAssertSubscriber<Integer> subscriber = makeHttpReqAsJson(httpClient, "/event/tooLongToExist")
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        int reqTime = subscriber.awaitItem(Duration.ofSeconds(TIMEOUT_SEC)).getItem();
        assertTrue(reqTime < KAFKA_MAX_BLOCK_TIME_MS, getErrorMsg(reqTime));
    }

    @Test
    public void kafkaProducerBlocksIfTopicsNotExistEmitterWithoutMetadata() {

        UniAssertSubscriber<Integer> subscriber = makeHttpReqAsJson(httpClient, "/event")
                .subscribe().withSubscriber(UniAssertSubscriber.create());

        int reqTime = subscriber.awaitItem(Duration.ofSeconds(TIMEOUT_SEC)).getItem();
        assertTrue(reqTime < KAFKA_MAX_BLOCK_TIME_MS, getErrorMsg(reqTime));
    }

    @Test
    public void severalEventsProducedKeepResponseTimes() {
        for (int i = 0; i < EVENTS; i++) {
            UniAssertSubscriber<Integer> subscriber = makeHttpReqAsJson(httpClient, "/event")
                    .subscribe().withSubscriber(UniAssertSubscriber.create());

            int reqTime = subscriber.awaitItem(Duration.ofSeconds(TIMEOUT_SEC)).getItem();
            assertTrue(reqTime < KAFKA_MAX_BLOCK_TIME_MS, getErrorMsg(reqTime));
        }
    }

    private Uni<Integer> makeHttpReqAsJson(WebClient httpClient, String path) {
        return httpClient.postAbs(getAppEndpoint() + path).send()
                .map(resp -> Integer.parseInt(resp.getHeader("x-ms")));
    }

    private String getAppEndpoint() {
        return String.format("http://localhost:%d/", app.getPort());
    }

    private String getErrorMsg(long reqTime) {
        return String.format("reqTime %d greater than KafkaMaxBlockMs %d", reqTime, KAFKA_MAX_BLOCK_TIME_MS);
    }
}
