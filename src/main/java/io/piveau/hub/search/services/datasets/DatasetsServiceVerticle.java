package io.piveau.hub.search.services.datasets;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.connector.CatalogueConnector;
import io.piveau.hub.search.util.connector.DatasetConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class DatasetsServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject config = ConfigHelper.forConfig(config()).forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_ES_CONFIG);

        Promise<DatasetConnector> datasetConnectorFuture = Promise.promise();
        DatasetConnector.create(vertx, config, datasetConnectorReady -> {
            if (datasetConnectorReady.succeeded()) {
                datasetConnectorFuture.complete(datasetConnectorReady.result());
            } else {
                datasetConnectorFuture.fail(datasetConnectorReady.cause());
            }
        });

        Promise<CatalogueConnector> catalogueConnectorFuture = Promise.promise();
        CatalogueConnector.create(vertx, config, catalogueConnectorReady -> {
            if (catalogueConnectorReady.succeeded()) {
                catalogueConnectorFuture.complete(catalogueConnectorReady.result());
            } else {
                catalogueConnectorFuture.fail(catalogueConnectorReady.cause());
            }
        });

        CompositeFuture.all(datasetConnectorFuture.future(), catalogueConnectorFuture.future()).setHandler(ar -> {
            if (ar.succeeded()) {
                DatasetsService.create(datasetConnectorFuture.future().result(), catalogueConnectorFuture.future().result(),
                        serviceReady -> {
                            if (serviceReady.succeeded()) {
                                new ServiceBinder(vertx).setAddress(DatasetsService.SERVICE_ADDRESS)
                                        .register(DatasetsService.class, serviceReady.result());
                                startPromise.complete();
                            } else {
                                startPromise.fail(serviceReady.cause());
                            }
                        });
            } else {
                startPromise.fail(ar.cause());
            }
        });
    }
}
