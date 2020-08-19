package io.piveau.hub.search.services.sitemaps;

import io.piveau.hub.search.util.connector.SitemapConnector;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface SitemapsService {
    String SERVICE_ADDRESS = "de.fhg.fokus.piveau.hub.search.sitemaps.queue";

    static SitemapsService create(SitemapConnector sitemapConnector,
                                  Handler<AsyncResult<SitemapsService>> readyHandler) {
        return new SitemapsServiceImpl(sitemapConnector, readyHandler);
    }

    static SitemapsService createProxy(Vertx vertx, String address) {
        return new SitemapsServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    SitemapsService readSitemapIndex(Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    SitemapsService readSitemap(String sitemapId, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    SitemapsService triggerSitemapGeneration(Handler<AsyncResult<JsonObject>> handler);
}
