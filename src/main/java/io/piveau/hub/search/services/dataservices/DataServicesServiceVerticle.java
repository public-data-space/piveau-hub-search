package io.piveau.hub.search.services.dataservices;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.connector.DataServiceConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class DataServicesServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_ES_CONFIG);

        DataServiceConnector.create(vertx, config, dataServiceConnectorReady -> {
            if (dataServiceConnectorReady.succeeded()) {
                DataServicesService.create(dataServiceConnectorReady.result(), serviceReady -> {
                    if (serviceReady.succeeded()) {
                        new ServiceBinder(vertx).setAddress(DataServicesService.SERVICE_ADDRESS)
                                .register(DataServicesService.class, serviceReady.result());
                        startPromise.complete();
                    } else {
                        startPromise.fail(serviceReady.cause());
                    }
                });
            } else {
                startPromise.fail(dataServiceConnectorReady.cause());
            }
        });
    }
}
