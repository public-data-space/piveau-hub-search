package io.piveau.hub.search.handler;

import io.piveau.hub.search.services.sitemaps.SitemapsService;
import io.vertx.core.*;
import io.vertx.ext.web.RoutingContext;

public class SitemapHandler extends ContextHandler {

    private SitemapsService sitemapsService;

    public SitemapHandler(Vertx vertx, String address) {
        sitemapsService = SitemapsService.createProxy(vertx, address);
    }
    public void readSitemapIndex(RoutingContext context)  {
        sitemapsService.readSitemapIndex(ar -> handleContextXML(context, ar));
    }

    public void readSitemap(RoutingContext context) {
        sitemapsService.readSitemap(context.request().getParam("id"), ar -> handleContextXML(context, ar));
    }
}
