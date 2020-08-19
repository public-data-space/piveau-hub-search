package io.piveau.hub.search.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

class ContextHandler {

    void handleContext(RoutingContext context, AsyncResult<JsonObject> ar) {
        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().putHeader("Access-Control-Allow-Origin", "*");
        if (ar.succeeded()) {
            response.put("success", true);
            response.put("result", ar.result().getJsonObject("result"));
            Integer status = (Integer) ar.result().remove("status");
            context.response().setStatusCode(status);
        } else {
            response.put("success", false);
            JsonObject result = new JsonObject(ar.cause().getMessage());
            response.put("result", result);
            Integer status = (Integer) result.remove("status");
            context.response().setStatusCode(status);
        }
        context.response().end(response.toString());
    }

    void handleContextXML(RoutingContext context, AsyncResult<JsonObject> ar) {
        if (ar.succeeded()) {
            context.response().putHeader("Content-Type", "application/xml");
            Integer status = (Integer) ar.result().remove("status");
            context.response().setStatusCode(status);
            context.response().end(ar.result().getString("result"));
        } else {
            JsonObject response = new JsonObject();
            context.response().putHeader("Content-Type", "application/json");
            context.response().putHeader("Access-Control-Allow-Origin", "*");
            response.put("success", false);
            JsonObject result = new JsonObject(ar.cause().getMessage());
            response.put("result", result);
            Integer status = (Integer) result.remove("status");
            context.response().setStatusCode(status);
            context.response().end(response.toString());
        }
    }
}
