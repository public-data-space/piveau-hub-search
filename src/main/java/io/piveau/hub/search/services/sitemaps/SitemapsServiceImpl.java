package io.piveau.hub.search.services.sitemaps;

import io.piveau.hub.search.util.connector.SitemapConnector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public class SitemapsServiceImpl implements SitemapsService {

    private SitemapConnector sitemapConnector;

    SitemapsServiceImpl(SitemapConnector sitemapConnector, Handler<AsyncResult<SitemapsService>> handler) {
        this.sitemapConnector = sitemapConnector;
        handler.handle(Future.succeededFuture(this));
    }

    @Override
    public SitemapsService readSitemapIndex(Handler<AsyncResult<JsonObject>> handler) {
        sitemapConnector.readSitemapIndex(ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public SitemapsService readSitemap(String sitemapId, Handler<AsyncResult<JsonObject>> handler) {
        sitemapConnector.readSitemap(sitemapId, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public SitemapsService triggerSitemapGeneration(Handler<AsyncResult<JsonObject>> handler) {
        sitemapConnector.triggerSitemapGeneration(ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

}
