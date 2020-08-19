package io.piveau.hub.search.util.gazetteer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.List;

// Integration from: https://gitlab.com/european-data-portal/ckanext-edp/blob/master/ckanext/eodp/gazetteer.py

class GazetteerConnectorConterra extends GazetteerConnector {

    GazetteerConnectorConterra(WebClient client, JsonObject config, Handler<AsyncResult<GazetteerConnector>> handler) {
        super(client, config, handler);
    }

    @Override
    public String buildUrl(String q) {
        LOG.debug(this.url + "?q=" + q);
        return this.url + "?q=" + q;
    }

    @Override
    public JsonObject querySuggestion(JsonObject message) {
        JsonArray results = new JsonArray();
        JsonObject result = new JsonObject().put("results", results);

        if (message == null) {
            return result;
        }

        JsonObject response = message.getJsonObject("response");

        if (response == null) {
            return result;
        }

        JsonArray docs = response.getJsonArray("docs");

        if(docs == null) {
            return result;
        }

        for(Object doc : docs) {
            JsonObject docJson;

            try {
                docJson = new JsonObject(doc.toString());
            } catch(DecodeException e) {
                continue;
            }

            JsonObject location = new JsonObject();
            List<String> text = new ArrayList<>();

            String featureType = docJson.getString("featureType");
            if(featureType != null && !featureType.isEmpty()) {
                location.put("featureType", featureType);
            } else {
                continue;
            }

            String name = docJson.getString("name");
            if(name != null && !name.isEmpty()) {
                text.add(name);
            }

            String admunit1_name = docJson.getString("admunit1_name");
            if(admunit1_name != null && !admunit1_name.isEmpty()) {
                text.add(admunit1_name);
            }

            String admunit2_name = docJson.getString("admunit2_name");
            if(admunit2_name != null && !admunit2_name.isEmpty()) {
                text.add(admunit2_name);
            }

            if(text.size() > 0) {
                location.put("name", String.join(", ", text));
            } else {
                continue;
            }

            JsonArray geometry = docJson.getJsonArray("geometry");
            if(geometry != null && !geometry.isEmpty()) {
                String box = wktToBoundingBox(geometry.getString(0), featureType);
                if(box != null && !box.isEmpty()) {
                    location.put("geometry", box);
                }
            } else{
                continue;
            }

            results.add(location);
        }

        return result;
    }

    private String wktToBoundingBox(String value, String featureType) {
        WKTReader reader = new WKTReader();

        Geometry geometry;

        try {
            geometry = reader.read(value);
        } catch (ParseException e) {
            return null;
        }

        switch (geometry.getGeometryType()) {
            case "Polygon":
                LOG.debug(geometry.getBoundary().toText());
                return geometry.getBoundary().toText();
            case "MultiPolygon":
                LOG.debug(geometry.getBoundary().toText());
                return geometry.getBoundary().toText();
            case "GeometryCollection":
                LOG.debug(geometry.getBoundary().toText());
                return geometry.getBoundary().toText();
            case "Point":
                return pointToBoundingBox((Point) geometry, featureType);
            default:
                return null;
        }
    }

    private String pointToBoundingBox(Point point, String featureType) {

        double x = point.getX();
        double y = point.getY();

        double extend_x;
        double extend_y;

        switch (featureType) {
            case "PPL":
                extend_x = 0.25;
                break;
            case "ADM1":
                extend_x = 0.5;
                break;
            case "ADM2":
                extend_x = 0.25;
                break;
            default:
                extend_x = 0.25;
                break;
        }

        extend_y = extend_x * 2;

        double x1 = y - extend_y;
        double x2 = x - extend_x;
        double y1 = y + extend_y;
        double y2 = x + extend_x;

        List<String> box = new ArrayList<>();

        /*DecimalFormat df = new DecimalFormat("###.###########");

        String str_x1 = df.format(x1).contains(".") ? df.format(x1) : df.format(x1) + ".0";
        String str_x2 = df.format(x2).contains(".") ? df.format(x2) : df.format(x2) + ".0";
        String str_y1 = df.format(y1).contains(".") ? df.format(y1) : df.format(y1) + ".0";
        String str_y2 = df.format(y2).contains(".") ? df.format(y2) : df.format(y2) + ".0";*/

        box.add(String.valueOf(x1));
        box.add(String.valueOf(x2));
        box.add(String.valueOf(y1));
        box.add(String.valueOf(y2));

        return String.join(",", box);
    }
}
