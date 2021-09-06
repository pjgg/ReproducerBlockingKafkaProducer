package org.acme.reproducer;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerHandler;

@ApplicationScoped
public class Application {

    private static final Logger LOG = Logger.getLogger(Application.class);

    @ConfigProperty(name = "app.name")
    public String serviceName;

    @Inject
    FailureHandler failureHandler;

    @Inject
    KafkaBlockingProducer producer;

    Router router;

    void init(@Observes Router router) {
        this.router = router;
    }

    void onStart(@Observes StartupEvent ev) {
        LOG.info(String.format("Application %s starting...", serviceName));
        addRoute(HttpMethod.POST, "/event/:topic", rc -> producer.pushEventToTopic(rc));
        addRoute(HttpMethod.POST, "/event", rc -> producer.pushEvent(rc));
        addRoute(HttpMethod.GET, "/hello", rc -> producer.hello(rc));
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOG.info(String.format("Application %s stopping...", serviceName));
    }

    private void addRoute(HttpMethod method, String path, Handler<RoutingContext> handler) {
        Route route = this.router.route(method, path)
                .handler(CorsHandler.create("*"))
                .handler(LoggerHandler.create());

        if (method.equals(HttpMethod.POST) || method.equals(HttpMethod.PUT))
            route.handler(BodyHandler.create());

        route.handler(handler).failureHandler(rc -> failureHandler.handler(rc));
    }
}
