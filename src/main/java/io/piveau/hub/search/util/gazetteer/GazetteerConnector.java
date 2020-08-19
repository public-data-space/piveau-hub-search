package io.piveau.hub.search.util.gazetteer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.piveau.hub.search.util.response.ReturnHelper.returnFailure;

public abstract class GazetteerConnector {

    Logger LOG = LoggerFactory.getLogger(GazetteerConnector.class);

    // Gezetteer url
    String url;

    // Webclient
    WebClient client;

    public static GazetteerConnector create(WebClient client, JsonObject config, Handler<AsyncResult<GazetteerConnector>> handler) {
        if(config.isEmpty()) {
            handler.handle(Future.failedFuture("No gazetteer config provided"));
        } else if(config.getString("url") == null || config.getString("url").isEmpty()) {
            handler.handle(Future.failedFuture("No gazetteer url provided"));
        } else {
            String type = config.getString("type");
            if (type == null || type.isEmpty()) {
                return new GazetteerConnectorConterra(client, config, handler);
            } else {
                switch(type) {
                    case "conterra":
                        return new GazetteerConnectorConterra(client, config, handler);
                    case "osmnames":
                        return new GazetteerConnectorOsmnames(client, config, handler);
                    default:
                        return new GazetteerConnectorConterra(client, config, handler);
                }
            }
        }
        return null;
    }

    GazetteerConnector(WebClient client, JsonObject config, Handler<AsyncResult<GazetteerConnector>> handler) {
        this.client = client;
        if(config.getString("url") == null || config.getString("url").isEmpty()) {
            handler.handle(Future.failedFuture("No gazetteer url provided"));
        } else {
            this.url = config.getString("url");
            handler.handle(Future.succeededFuture(this));
        }
    }

    public void autocomplete(String q, Handler<AsyncResult<JsonObject>> handler) {
        if(q == null || q.isEmpty()) {
            handler.handle(Future.failedFuture(returnFailure(404, "Query null or empty!")));
        } else {
            client.getAbs(buildUrl(q)).send(ar -> {
                if (ar.succeeded()) {
                    HttpResponse<Buffer> response = ar.result();

                    if(response.statusCode() != 200) {
                        LOG.error("Gezetteer autocomplete: Received response with status code "
                                + response.statusCode());
                        handler.handle(Future.failedFuture(returnFailure(response.statusCode(),
                                "Gazetteer Service does not respond proberly")));
                    } else {
                        try {
                            handler.handle(Future.succeededFuture(
                                    new JsonObject()
                                            .put("status", 200)
                                            .put("result", querySuggestion(new JsonObject(response.body().toString())))
                            ));
                        } catch (DecodeException e) {
                            LOG.error("Gezetteer autocomplete: Gazetteer Service didn't respond a json");
                            handler.handle(Future.failedFuture(returnFailure(500,
                                    "Gazetteer Service didn't respond a json")));
                        }
                    }
                } else {
                    LOG.error("Gezetteer autocomplete: " + ar.cause().getMessage());
                    handler.handle(Future.failedFuture(returnFailure(500, ar.cause().getMessage())));
                }
            });
        }
    }

    public abstract String buildUrl(String q);

    public abstract JsonObject querySuggestion(JsonObject message);
}
