package io.piveau.hub.search.services.dataservices;

import io.piveau.hub.search.util.connector.DataServiceConnector;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface DataServicesService {
    String SERVICE_ADDRESS = "de.fhg.fokus.piveau.hub.search.dataservice.queue";

    static DataServicesService create(DataServiceConnector dataServiceConnector,
                                      Handler<AsyncResult<DataServicesService>> readyHandler) {
        return new DataServicesServiceImpl(dataServiceConnector, readyHandler);
    }

    static DataServicesService createProxy(Vertx vertx, String address) {
        return new DataServicesServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    DataServicesService createDataService(JsonObject payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DataServicesService createOrUpdateDataService(String dataServiceId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DataServicesService modifyDataService(String dataServiceId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DataServicesService readDataService(String dataServiceId, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DataServicesService deleteDataService(String dataServiceId, Handler<AsyncResult<JsonObject>> handler);

}
