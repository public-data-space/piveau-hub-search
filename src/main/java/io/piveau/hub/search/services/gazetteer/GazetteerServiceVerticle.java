package io.piveau.hub.search.services.gazetteer;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.gazetteer.GazetteerConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;

public class GazetteerServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_GAZETTEER_CONFIG);

        WebClient client = WebClient.create(vertx);

        GazetteerConnector.create(client, config, connectorReady -> {
            if (connectorReady.succeeded()) {
                GazetteerService.create(connectorReady.result(), serviceReady -> {
                    if (serviceReady.succeeded()) {
                        new ServiceBinder(vertx).setAddress(GazetteerService.SERVICE_ADDRESS)
                                .register(GazetteerService.class, serviceReady.result());
                        startPromise.complete();
                    } else {
                        startPromise.fail(serviceReady.cause());
                    }
                });
            } else {
                startPromise.fail(connectorReady.cause());
            }
        });
    }
}
