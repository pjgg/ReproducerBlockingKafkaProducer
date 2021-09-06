package org.acme.reproducer;

import java.time.Duration;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;

@ApplicationScoped
public class KafkaBlockingProducer {

    @Inject
    @Channel("test")
    MutinyEmitter<String> emitter;

    public void pushEvent(final RoutingContext context) {
        emitter.send("ping")
                .onFailure().invoke(context::fail)
                .ifNoItem().after(Duration.ofSeconds(5)).failWith(new HttpException(408, "Producer blocked"))
                .subscribe().with(resp -> context.response()
                        .putHeader("Content-Type", "application/json")
                        .end("success"));

    }

    public void pushEventToTopic(final RoutingContext context) {
        String topic = context.request().getParam("topic");
        OutgoingKafkaRecordMetadata<?> metadata = OutgoingKafkaRecordMetadata.builder()
                .withTopic(topic)
                .withKey(UUID.randomUUID().toString())
                .build();

        emitter.send(Message.of("ping").addMetadata(metadata));
        context.response()
                .putHeader("Content-Type", "application/json")
                .end("success");
    }

    public void hello(final RoutingContext context) {
        context.response()
                .putHeader("Content-Type", "application/json")
                .end("hello World");
    }
}
