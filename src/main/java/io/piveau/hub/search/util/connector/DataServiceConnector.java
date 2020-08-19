package io.piveau.hub.search.util.connector;

import io.piveau.hub.search.util.index.IndexManager;
import io.piveau.hub.search.util.response.GetResponseHelper;
import io.piveau.hub.search.util.response.ReturnHelper;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DataServiceConnector {

    private static final Logger LOG = LoggerFactory.getLogger(DataServiceConnector.class);

    // elasticsearch client
    private RestHighLevelClient client;

    // index manager
    private IndexManager indexManager;

    public static DataServiceConnector create(Vertx vertx, JsonObject config, Handler<AsyncResult<DataServiceConnector>> handler) {
        return new DataServiceConnector(vertx, config, handler);
    }

    private DataServiceConnector(Vertx vertx, JsonObject config, Handler<AsyncResult<DataServiceConnector>> handler) {
        String host = config.getString("host", "localhost");
        Integer port = config.getInteger("port", 9200);

        this.client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, "http"))
        );

        IndexManager.create(vertx, config, ar -> {
            if (ar.succeeded()) {
                this.indexManager = ar.result();

                handler.handle(Future.succeededFuture(this));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void createDataService(JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        String dataServiceId = UUID.randomUUID().toString();

        payload.put("id", dataServiceId);

        JsonObject result = new JsonObject().put("id", dataServiceId);

        IndexRequest indexRequest = new IndexRequest("dataservice").id(dataServiceId).opType("create")
                .source(payload.encode(), XContentType.JSON);
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                LOG.info("Index dataservice: DataService {} created. {}", dataServiceId, indexResponse.toString());
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(201, result)));
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Index dataservice: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

    public void createOrUpdateDataService(String dataServiceId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        if (dataServiceId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (dataServiceId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        payload.put("id", dataServiceId);

        JsonObject result = new JsonObject().put("id", dataServiceId);

        IndexRequest indexRequest = new IndexRequest("dataservice").id(dataServiceId)
                .source(payload.encode(), XContentType.JSON);
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                if (indexResponse.status().getStatus() == 200) {
                    // updated
                    LOG.info("Index dataservice: DataService {} updated. {}", dataServiceId, indexResponse.toString());
                } else {
                    // created
                    LOG.info("Index dataservice: DataService {} created. {}", dataServiceId, indexResponse.toString());
                }
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(indexResponse.status().getStatus(), result)));
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Index dataservice: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

    public void modifyDataService(String dataServiceId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        if (dataServiceId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (dataServiceId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        JsonObject result = new JsonObject().put("id", dataServiceId);

        UpdateRequest updateRequest = new UpdateRequest("dataservice", dataServiceId)
                .doc(payload.toString(), XContentType.JSON);
        client.updateAsync(updateRequest, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                LOG.info("Index dataservice: DataService {} modified. {}", dataServiceId, updateResponse.toString());
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, result)));
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Index dataservice: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

    public void readDataService(String dataServiceId, Handler<AsyncResult<JsonObject>> handler) {
        if (dataServiceId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (dataServiceId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        GetRequest getRequest = new GetRequest("dataservice", dataServiceId);
        client.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {
                    handler.handle(Future.succeededFuture(
                            ReturnHelper.returnSuccess(200, GetResponseHelper.getResponseToJson(getResponse,
                                    indexManager.getFields().get("dataservice")))));
                } else {
                    LOG.error("Read dataservice: DataService {} not found", dataServiceId);
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(404,
                            "DataService " + dataServiceId + " not found")));
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Read dataservice: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

    public void deleteDataService(String dataServiceId, Handler<AsyncResult<JsonObject>> handler) {
        if (dataServiceId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (dataServiceId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        JsonObject result = new JsonObject().put("id", dataServiceId);

        DeleteRequest deleteRequest = new DeleteRequest("dataservice", dataServiceId);
        client.deleteAsync(deleteRequest, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.status() == RestStatus.NOT_FOUND) {
                    LOG.error("Delete dataservice: DataService {} not found", dataServiceId);
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(404,
                            "DataService " + dataServiceId + " not found")));
                } else {
                    handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, result)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Delete dataservice: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

}
