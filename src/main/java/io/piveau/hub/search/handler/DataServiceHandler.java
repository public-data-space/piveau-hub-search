package io.piveau.hub.search.handler;

import io.piveau.hub.search.services.dataservices.DataServicesService;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;

import java.util.List;

public class DataServiceHandler extends ContextHandler {

    private DataServicesService dataServicesService;

    public DataServiceHandler(Vertx vertx, String address) {
        dataServicesService = DataServicesService.createProxy(vertx, address);
    }

    public void createDataService(RoutingContext context) {
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            dataServicesService.createDataService(context.getBodyAsJson(), ar -> handleContext(context, ar));
        } else {
            dataServicesService.createDataService(context.getBodyAsJson(), ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void createOrUpdateDataService(RoutingContext context) {
        String id = context.request().getParam("id");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            dataServicesService.createOrUpdateDataService(id, context.getBodyAsJson(), ar -> handleContext(context, ar));
        } else {
            dataServicesService.createOrUpdateDataService(id, context.getBodyAsJson(), ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void modifyDataService(RoutingContext context) {
        String id = context.request().getParam("id");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            dataServicesService.modifyDataService(id, context.getBodyAsJson(), ar -> handleContext(context, ar));
        } else {
            dataServicesService.modifyDataService(id, context.getBodyAsJson(), ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void readDataService(RoutingContext context) {
        String id = context.request().getParam("id");
        dataServicesService.readDataService(id, ar -> handleContext(context, ar));
    }

    public void deleteDataService(RoutingContext context) {
        String id = context.request().getParam("id");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            dataServicesService.deleteDataService(id, ar -> handleContext(context, ar));
        } else {
            dataServicesService.deleteDataService(id, ar -> {});
            context.response().setStatusCode(202).end();
        }
    }
}
