package io.piveau.hub.search.services.catalogues;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.connector.CatalogueConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class CataloguesServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_ES_CONFIG);

        CatalogueConnector.create(vertx, config, catalogueConnectorReady -> {
            if (catalogueConnectorReady.succeeded()) {
                CataloguesService.create(catalogueConnectorReady.result(), serviceReady -> {
                    if (serviceReady.succeeded()) {
                        new ServiceBinder(vertx).setAddress(CataloguesService.SERVICE_ADDRESS)
                                .register(CataloguesService.class, serviceReady.result());
                        startPromise.complete();
                    } else {
                        startPromise.fail(serviceReady.cause());
                    }
                });
            } else {
                startPromise.fail(catalogueConnectorReady.cause());
            }
        });
    }
}
