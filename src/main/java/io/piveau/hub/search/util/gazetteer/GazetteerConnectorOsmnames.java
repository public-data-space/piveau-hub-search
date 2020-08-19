package io.piveau.hub.search.util.gazetteer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;

import java.util.ArrayList;
import java.util.List;

class GazetteerConnectorOsmnames extends GazetteerConnector {

    GazetteerConnectorOsmnames(WebClient client, JsonObject config, Handler<AsyncResult<GazetteerConnector>> handler) {
        super(client, config, handler);
    }

    @Override
    public String buildUrl(String q) {
        LOG.debug(this.url + q + ".js");
        return this.url + q + ".js";
    }

    @Override
    public JsonObject querySuggestion(JsonObject message) {
        JsonArray results = new JsonArray();
        JsonObject result = new JsonObject().put("results", results);

        if (message == null) {
            return result;
        }

        JsonArray docs = message.getJsonArray("results");

        if(docs == null) {
            return result;
        }

        for(Object doc : docs) {

            JsonObject docJson;

            try {
                docJson = new JsonObject(doc.toString());
            } catch(DecodeException e) {
                LOG.error(e.getMessage());
                continue;
            }

            JsonObject location = new JsonObject();

            String featureType = docJson.getString("type");
            if(featureType != null && !featureType.isEmpty()) {
                location.put("featureType", featureType);
            } else {
                continue;
            }

            String name = docJson.getString("display_name");
            if(name != null && !name.isEmpty()) {
                location.put("name", name);
            } else {
                continue;
            }

            JsonArray geometry = docJson.getJsonArray("boundingbox");
            if(geometry != null && !geometry.isEmpty() && geometry.size() == 4) {
                List<String> box = new ArrayList<>();

                box.add(String.valueOf(geometry.getFloat(0)));
                box.add(String.valueOf(geometry.getFloat(1)));
                box.add(String.valueOf(geometry.getFloat(2)));
                box.add(String.valueOf(geometry.getFloat(3)));

                location.put("geometry", String.join(",", box));
            } else{
                continue;
            }

            results.add(location);
        }

        return result;
    }
}
