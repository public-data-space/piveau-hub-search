package io.piveau.hub.search.services.search;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.connector.ElasticsearchConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class SearchServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_ES_CONFIG);

        ElasticsearchConnector.create(vertx, config, connectorReady -> {
            if (connectorReady.succeeded()) {
                SearchService.create(connectorReady.result(), serviceReady -> {
                    if (serviceReady.succeeded()) {
                        new ServiceBinder(vertx).setAddress(SearchService.SERVICE_ADDRESS)
                                .register(SearchService.class, serviceReady.result());
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
