package io.piveau.hub.search.services.dataservices;

import io.piveau.hub.search.util.connector.DataServiceConnector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public class DataServicesServiceImpl implements DataServicesService {

    private DataServiceConnector dataServiceConnector;

    DataServicesServiceImpl(DataServiceConnector dataServiceConnector, Handler<AsyncResult<DataServicesService>> handler) {
        this.dataServiceConnector = dataServiceConnector;
        handler.handle(Future.succeededFuture(this));
    }

    @Override
    public DataServicesService createDataService(JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        dataServiceConnector.createDataService(payload, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DataServicesService createOrUpdateDataService(String dataServiceId, JsonObject payload,
                                                       Handler<AsyncResult<JsonObject>> handler) {
        dataServiceConnector.createOrUpdateDataService(dataServiceId, payload, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DataServicesService modifyDataService(String dataServiceId, JsonObject payload,
                                               Handler<AsyncResult<JsonObject>> handler) {
        dataServiceConnector.modifyDataService(dataServiceId, payload, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DataServicesService readDataService(String dataServiceId, Handler<AsyncResult<JsonObject>> handler) {
        dataServiceConnector.readDataService(dataServiceId, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DataServicesService deleteDataService(String dataServiceId, Handler<AsyncResult<JsonObject>> handler) {
        dataServiceConnector.deleteDataService(dataServiceId, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

}
