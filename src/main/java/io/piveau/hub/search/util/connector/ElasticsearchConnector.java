package io.piveau.hub.search.util.connector;

import io.piveau.hub.search.util.index.IndexManager;
import io.piveau.hub.search.util.response.GetResponseHelper;
import io.piveau.hub.search.util.response.ReturnHelper;
import io.piveau.hub.search.util.search.SearchRequestHelper;
import io.piveau.hub.search.util.search.SearchResponseHelper;
import io.piveau.hub.search.util.request.Field;
import io.piveau.hub.search.util.request.Query;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.circuitbreaker.CircuitBreakerOptions;
import io.vertx.core.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ElasticsearchConnector {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchConnector.class);

    // elasticsearch client
    private RestHighLevelClient client;

    // index manager
    private IndexManager indexManager;

    // circuit breaker
    private CircuitBreaker breaker;

    public static ElasticsearchConnector create(Vertx vertx, JsonObject config,
                                                Handler<AsyncResult<ElasticsearchConnector>> handler) {
        return new ElasticsearchConnector(vertx, config, handler);
    }

    private ElasticsearchConnector(Vertx vertx, JsonObject config,
                                   Handler<AsyncResult<ElasticsearchConnector>> handler) {

        String host = config.getString("host", "localhost");
        Integer port = config.getInteger("port", 9200);
        Integer circuitBreakerTries = config.getInteger("circuitBreakerTries", 10);

        this.client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, "http"))
        );

        this.breaker = CircuitBreaker.create("elasticsearch-breaker", vertx,
                new CircuitBreakerOptions()
                        .setMaxRetries(circuitBreakerTries)
                        .setTimeout(-1)
        ).retryPolicy(retryCount -> {
            LOG.info("Try number " + retryCount);
            return retryCount * 1000L;
        });

        IndexManager.create(vertx, config, ar -> {
            if (ar.succeeded()) {
                this.indexManager = ar.result();
                this.ping(pingResult -> {
                    if (pingResult.succeeded()) {
                        this.initIndex("dataset");
                        this.initIndex("catalogue");
                        this.initIndex("dataservice");
                        handler.handle(Future.succeededFuture(this));
                    } else {
                        handler.handle(Future.failedFuture(pingResult.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void ping(Handler<AsyncResult<String>> handler) {
        breaker.execute(
                future -> {
                    try {
                        this.client.ping(RequestOptions.DEFAULT);
                        LOG.info("Succeeded to ping elasticsearch");
                        future.complete();
                    } catch (IOException e) {
                        LOG.error("Failed to ping elasticsearch");
                        future.fail(e.getCause());
                    }
                }).setHandler(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture());
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public IndexManager getIndexManager() {
        return indexManager;
    }

    private void initIndex(String index) {
        indexCreate(index, indexCreateResult -> {
            if (!indexCreateResult.succeeded()) {
                LOG.error(indexCreateResult.cause().getMessage());
            }

            putMapping(index, putMappingResult -> {
                if (putMappingResult.succeeded()) {
                    setMaxResultWindow(index, indexManager.getMaxResultWindow().get(index),
                            setMaxResultWindowResult -> {
                                if (!setMaxResultWindowResult.succeeded()) {
                                    LOG.error(setMaxResultWindowResult.cause().getMessage());
                                }
                            });
                } else {
                    LOG.error(putMappingResult.cause().getMessage());
                }
            });
        });
    }

    public void indexCreate(String index, Handler<AsyncResult<String>> handler) {
        indexManager.prepareIndexCreateRequest(index, ar -> {
            if (ar.succeeded()) {
                client.indices().createAsync(ar.result(), RequestOptions.DEFAULT,
                        new ActionListener<>() {

                            @Override
                            public void onResponse(CreateIndexResponse createIndexResponse) {
                                if (createIndexResponse.isAcknowledged()
                                        && createIndexResponse.isShardsAcknowledged()) {
                                    handler.handle(Future.succeededFuture(
                                            "The index was successfully created (" + index + ")"));
                                } else {
                                    handler.handle(Future.failedFuture("Failed to create index (" + index + ")"));
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                handler.handle(Future.failedFuture(e));
                            }
                        });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void indexDelete(String index, Handler<AsyncResult<String>> handler) {
        indexManager.prepareIndexDeleteRequest(index, ar -> {
            if (ar.succeeded()) {
                client.indices().deleteAsync(ar.result(), RequestOptions.DEFAULT,
                        new ActionListener<>() {
                            @Override
                            public void onResponse(AcknowledgedResponse deleteIndexResponse) {
                                if (deleteIndexResponse.isAcknowledged()) {
                                    handler.handle(Future.succeededFuture(
                                            "The index was successfully deleted (" + index + ")"));
                                } else {
                                    handler.handle(Future.failedFuture("Failed to delete index (" + index + ")"));
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                handler.handle(Future.failedFuture(e));
                            }
                        });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void putMapping(String index, Handler<AsyncResult<String>> handler) {
        indexManager.preparePutMappingRequest(index, ar -> {
            if (ar.succeeded()) {
                client.indices().putMappingAsync(ar.result(), RequestOptions.DEFAULT,
                        new ActionListener<>() {
                            @Override
                            public void onResponse(AcknowledgedResponse putMappingResponse) {
                                if (putMappingResponse.isAcknowledged()) {
                                    handler.handle(Future.succeededFuture(
                                            "The mapping was successfully added (" + index + ")"));
                                } else {
                                    handler.handle(Future.failedFuture("Failed to put mapping (" + index + ")"));
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                handler.handle(Future.failedFuture(e));
                            }
                        });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void setMaxResultWindow(String index, Integer max_result_window,
                                   Handler<AsyncResult<String>> handler) {
        indexManager.prepareSetMaxResultWindowRequest(index, max_result_window, ar -> {
            if (ar.succeeded()) {
                client.indices().putSettingsAsync(ar.result(), RequestOptions.DEFAULT,
                        new ActionListener<>() {

                            @Override
                            public void onResponse(AcknowledgedResponse updateSettingsResponse) {
                                if (updateSettingsResponse.isAcknowledged()) {
                                    handler.handle(Future.succeededFuture("Successfully set max_result_window = "
                                            + max_result_window));
                                    indexManager.setMaxResultWindow(index, max_result_window);
                                } else {
                                    handler.handle(Future.failedFuture("Failed to set max_result_window"));
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                handler.handle(Future.failedFuture(e));
                            }
                        });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void search(String q, Handler<AsyncResult<JsonObject>> handler) {
        Query query = Json.decodeValue(q, Query.class);

        LOG.info(query.toString());

        String filter = query.getFilter();

        SearchRequest searchRequest;
        SearchRequest aggregationRequest;

        if (Strings.isNullOrEmpty(filter) || filter.equals("autocomplete")) {
            searchRequest = SearchRequestHelper.buildSearchRequest(query);
            aggregationRequest = null;
        } else {
            Integer maxResultWindow = indexManager.getMaxResultWindow().get(filter);
            Integer maxAggSize = indexManager.getMaxAggSize().get(filter);
            Map<String, Field> fields = indexManager.getFields().get(filter);
            Map<String, ImmutablePair<String, String>> facets = indexManager.getFacets().get(filter);
            Map<String, String> searchParams = indexManager.getSearchParams().get(filter);

            searchRequest = SearchRequestHelper
                    .buildSearchRequest(query, maxResultWindow, fields, facets, searchParams);

            if (query.isAggregation()) {
                aggregationRequest = SearchRequestHelper
                        .buildAggregationRequest(query, maxAggSize, fields, facets, searchParams);
            } else {
                aggregationRequest = null;
            }
        }

        Promise<SearchResponse> search = doSearch(searchRequest);
        Promise<SearchResponse> agg = doSearch(aggregationRequest);

        CompositeFuture.all(search.future(), agg.future()).setHandler(ar -> {
            if (ar.succeeded()) {
                buildResult(search.future().result(), agg.future().result(), query,
                        buildResultAr -> handler.handle(Future.succeededFuture(
                                ReturnHelper.returnSuccess(200, buildResultAr.result()))));
            } else {
                handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, ar.cause().getMessage())));
            }
        });
    }

    public void scroll(String scrollId, Handler<AsyncResult<JsonObject>> handler) {
        LOG.info(scrollId);

        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(TimeValue.timeValueSeconds(60));
        Promise<SearchResponse> search = doScroll(scrollRequest);

        Query query = new Query();
        query.setScroll(true);

        search.future().setHandler(ar -> {
            if (ar.succeeded()) {
                buildResult(search.future().result(), null, query,
                        buildResultAr -> handler.handle(Future.succeededFuture(
                                ReturnHelper.returnSuccess(200, buildResultAr.result()))));
            } else {
                handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, ar.cause().getMessage())));
            }
        });
    }

    private Promise<SearchResponse> doScroll(SearchScrollRequest searchScrollRequest) {
        Promise<SearchResponse> promise = Promise.promise();
        if (searchScrollRequest == null) {
            promise.complete(null);
        } else {
            client.scrollAsync(searchScrollRequest, RequestOptions.DEFAULT, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    promise.complete(searchResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    LOG.error("Search: " + e);
                    e.printStackTrace();
                    promise.fail(e);
                }
            });
        }
        return promise;
    }

    private Promise<SearchResponse> doSearch(SearchRequest searchRequest) {
        Promise<SearchResponse> promise = Promise.promise();
        if (searchRequest == null) {
            promise.complete(null);
        } else {
            client.searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    promise.complete(searchResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    LOG.error("Search: " + e);
                    e.printStackTrace();
                    promise.fail(e);
                }
            });
        }
        return promise;
    }

    private void countDatasets(String query, Handler<AsyncResult<Long>> handler) {
        QueryBuilder termQuery = QueryBuilders.termQuery("catalog.id", query);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(termQuery);

        CountRequest countRequest = new CountRequest("dataset");
        countRequest.source(searchSourceBuilder);

        client.countAsync(countRequest, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(CountResponse countResponse) {
                handler.handle(Future.succeededFuture(countResponse.getCount()));
            }

            @Override
            public void onFailure(Exception e) {
                handler.handle(Future.failedFuture(e));
            }
        });
    }

    private void getCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        if (catalogueId == null) {
            handler.handle(Future.failedFuture("ID is null"));
            return;
        }

        if (catalogueId.isEmpty()) {
            handler.handle(Future.failedFuture("ID is empty"));
            return;
        }

        GetRequest getRequest = new GetRequest("catalogue", catalogueId);
        client.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {
                    handler.handle(Future.succeededFuture(GetResponseHelper.getResponseToJson(getResponse,
                            indexManager.getFields().get("catalogue"))));
                } else {
                    LOG.error("Read catalogue: Catalogue {} not found", catalogueId);
                    handler.handle(Future.failedFuture("Catalogue " + catalogueId + " not found"));
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Read catalogue: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(statusException.getMessage()));
                } else {
                    handler.handle(Future.failedFuture(e.getMessage()));
                }
            }
        });
    }

    private void buildResult(SearchResponse searchResponse, SearchResponse aggregationResponse, Query query,
                             Handler<AsyncResult<JsonObject>> handler) {
        JsonObject result = new JsonObject();

        result.put("count", searchResponse.getHits().getTotalHits().value);

        if (query.isScroll()) {
            result.put("scrollId", searchResponse.getScrollId());
        }

        List<Future> futureList = new ArrayList<>();

        if (aggregationResponse != null && aggregationResponse.getAggregations() != null) {
            JsonArray facets = SearchResponseHelper
                    .processAggregationResult(query, aggregationResponse,
                            indexManager.getFacetOrder().get(query.getFilter()));
            result.put("facets", facets);

            for (Object facet : facets) {
                JsonObject facetJson = (JsonObject) facet;
                if (facetJson.getString("id") != null && facetJson.getString("id").equals("catalog")) {
                    for (Object item : facetJson.getJsonArray("items")) {
                        JsonObject itemJson = (JsonObject) item;
                        String catalogueId = itemJson.getString("id");
                        JsonObject dataset = new JsonObject();
                        Promise catalogPromise = Promise.promise();
                        if (!Strings.isNullOrEmpty(catalogueId)) {
                            getCatalogue(catalogueId, getCatalogueResult -> {
                                if (getCatalogueResult.succeeded()) {
                                    dataset.put("catalog", getCatalogueResult.result());
                                }
                                itemJson.put("title", SearchResponseHelper.getCatalogTitle(dataset, catalogueId));
                                catalogPromise.complete();
                            });
                            futureList.add(catalogPromise.future());
                        }
                    }
                    break;
                }
            }
        }

        JsonArray datasets = new JsonArray();
        JsonArray countDatasets = new JsonArray();

        JsonArray results = SearchResponseHelper.processSearchResult(
                searchResponse,
                query,
                indexManager.getFacets().get("dataset"),
                datasets,
                countDatasets,
                indexManager.getFields().get(query.getFilter())
        );

        result.put("results", results);

        for (Object value : datasets) {
            JsonObject hitResult = (JsonObject) value;

            Promise<Void> catalogPromise = Promise.promise();
            JsonObject catalog = hitResult.getJsonObject("catalog");
            if (catalog != null && !catalog.isEmpty()) {
                String catalogueId = catalog.getString("id");
                if (!Strings.isNullOrEmpty(catalogueId)) {
                    getCatalogue(catalogueId, getCatalogueResult -> {
                        if (getCatalogueResult.succeeded()) {
                            hitResult.put("catalog", getCatalogueResult.result());
                        }
                        catalogPromise.complete();
                    });
                    futureList.add(catalogPromise.future());
                }
            }
        }

        for (Object value : countDatasets) {
            JsonObject hitResult = (JsonObject) value;

            Promise<Void> countPromise = Promise.promise();
            countDatasets(hitResult.getString("id"), countDatasetsResult -> {
                if (countDatasetsResult.succeeded()) {
                    hitResult.put("count", countDatasetsResult.result());
                } else {
                    hitResult.putNull("count");
                }
                countPromise.complete();
            });
            futureList.add(countPromise.future());
        }

        CompositeFuture.all(futureList).setHandler(ar -> handler.handle(Future.succeededFuture(result)));
    }
}
