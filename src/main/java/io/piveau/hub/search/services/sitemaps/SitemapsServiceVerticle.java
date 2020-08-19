package io.piveau.hub.search.services.sitemaps;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.connector.SitemapConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class SitemapsServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject esConfig = ConfigHelper.forConfig(config())
                .forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_ES_CONFIG);
        JsonObject sitemapConfig = ConfigHelper.forConfig(config())
                .forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_SITEMAP_CONFIG);

        SitemapConnector.create(vertx, sitemapConfig, esConfig, sitemapConnectorReady -> {
            if (sitemapConnectorReady.succeeded()) {
                SitemapsService.create(sitemapConnectorReady.result(), serviceReady -> {
                    if (serviceReady.succeeded()) {
                        new ServiceBinder(vertx).setAddress(SitemapsService.SERVICE_ADDRESS)
                                .register(SitemapsService.class, serviceReady.result());
                        startPromise.complete();
                    } else {
                        startPromise.fail(serviceReady.cause());
                    }
                });
            } else {
                startPromise.fail(sitemapConnectorReady.cause());
            }
        });
    }
}
