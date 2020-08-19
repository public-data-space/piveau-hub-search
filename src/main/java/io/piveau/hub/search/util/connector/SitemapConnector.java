package io.piveau.hub.search.util.connector;

import io.piveau.hub.search.util.index.IndexManager;
import io.piveau.hub.search.util.request.Field;
import io.piveau.hub.search.util.request.Query;
import io.piveau.hub.search.util.response.ReturnHelper;
import io.piveau.hub.search.util.search.SearchRequestHelper;
import io.piveau.hub.search.util.search.SearchResponseHelper;
import io.piveau.hub.search.util.sitemap.Sitemap;
import io.piveau.hub.search.util.sitemap.SitemapIndex;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystemException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SitemapConnector {

    private static final Logger LOG = LoggerFactory.getLogger(SitemapConnector.class);

    // elasticsearch client
    private RestHighLevelClient client;

    // index manager
    private IndexManager indexManager;

    // path for sitemap xml
    private String writepath;
    private String readpath;

    // sitemap config
    private JsonObject sitemapConfig;

    // vertx
    private Vertx vertx;

    // executer
    private WorkerExecutor executor;

    public static SitemapConnector create(Vertx vertx, JsonObject sitemapConfig, JsonObject esConfig,
                                          Handler<AsyncResult<SitemapConnector>> handler) {
        return new SitemapConnector(vertx, sitemapConfig, esConfig, handler);
    }

    private SitemapConnector(Vertx vertx, JsonObject sitemapConfig, JsonObject esConfig, Handler<AsyncResult<SitemapConnector>> handler) {
        String host = esConfig.getString("host", "localhost");
        Integer port = esConfig.getInteger("port", 9200);

        this.client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, "http"))
        );

        this.sitemapConfig = sitemapConfig;
        this.vertx = vertx;

        this.executor = vertx.createSharedWorkerExecutor("sitemaps-worker-pool", 3, 3, TimeUnit.MINUTES);

        IndexManager.create(vertx, esConfig, indexManagerAsyncResult -> {
            if (indexManagerAsyncResult.succeeded()) {
                this.indexManager = indexManagerAsyncResult.result();

                Promise<Void> createFolder1 = Promise.promise();
                Promise<Void> createFolder2 = Promise.promise();

                vertx.fileSystem().mkdir("./sitemaps", createSitemapFolder -> {
                    if (createSitemapFolder.succeeded() ||
                            createSitemapFolder.cause().getCause() instanceof FileAlreadyExistsException) {
                        vertx.fileSystem().mkdir("./sitemaps/1", createSitemap1Folder -> {
                            if (createSitemap1Folder.succeeded() ||
                                    createSitemap1Folder.cause().getCause() instanceof  FileAlreadyExistsException) {
                                createFolder1.complete();
                            } else {
                                createSitemap1Folder.cause().printStackTrace();
                                createFolder1.fail(createSitemap1Folder.cause());
                            }
                        });

                        vertx.fileSystem().mkdir("./sitemaps/2", createSitemap2Folder -> {
                            if (createSitemap2Folder.succeeded() ||
                                    createSitemap2Folder.cause().getCause() instanceof FileAlreadyExistsException) {
                                createFolder2.complete();
                            } else {
                                createSitemap2Folder.cause().printStackTrace();
                                createFolder2.fail(createSitemap2Folder.cause());
                            }
                        });
                    } else {
                        createSitemapFolder.cause().printStackTrace();
                        createFolder1.fail(createSitemapFolder.cause());
                        createFolder2.fail(createSitemapFolder.cause());
                    }
                });

                CompositeFuture.all(createFolder1.future(), createFolder2.future()).setHandler(createFoldersResult -> {
                    if (createFoldersResult.succeeded()
                            || createFoldersResult.cause() instanceof FileAlreadyExistsException) {
                        Promise<Long> sitemap1 = Promise.promise();
                        Promise<Long> sitemap2 = Promise.promise();

                        vertx.fileSystem().props("./sitemaps/1/sitemap_index.xml", ar -> {
                            if (ar.succeeded()) {
                                sitemap1.complete(ar.result().lastModifiedTime());
                            } else {
                                sitemap1.fail(ar.cause());
                            }
                        });

                        vertx.fileSystem().props("./sitemaps/2/sitemap_index.xml", ar -> {
                            if (ar.succeeded()) {
                                sitemap2.complete(ar.result().lastModifiedTime());
                            } else {
                                sitemap2.fail(ar.cause());
                            }
                        });

                        CompositeFuture.all(sitemap1.future(), sitemap2.future()).setHandler(ar -> {
                            if (sitemap1.future().succeeded() && sitemap2.future().succeeded()) {
                                if (sitemap1.future().result() <= sitemap2.future().result()) {
                                    writepath = "./sitemaps/1/";
                                    readpath = "./sitemaps/2/";
                                } else {
                                    writepath = "./sitemaps/2/";
                                    readpath = "./sitemaps/1/";
                                }
                            } else if (sitemap1.future().succeeded()) {
                                writepath = "./sitemaps/2/";
                                readpath = "./sitemaps/1/";
                            } else if (sitemap2.future().succeeded()) {
                                writepath = "./sitemaps/1/";
                                readpath = "./sitemaps/2/";
                            } else {
                                writepath = "./sitemaps/1/";
                                readpath = "./sitemaps/2/";
                            }
                            // start periodic sitemap generation
                            vertx.setPeriodic(sitemapConfig.getInteger("interval", 86400000), periodic ->
                                executor.executeBlocking(promise -> {
                                    generateSitemaps();
                                    promise.complete();
                                }, res -> {})
                            );
                        });
                        handler.handle(Future.succeededFuture(this));
                    } else {
                        handler.handle(Future.failedFuture(createFoldersResult.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(indexManagerAsyncResult.cause()));
            }
        });
    }

    public void readSitemapIndex(Handler<AsyncResult<JsonObject>> handler)  {
        vertx.fileSystem().readFile(readpath + "sitemap_index.xml", ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, ar.result().toString())));
            } else {
                if (ar.cause() instanceof FileSystemException && ar.cause().getCause() instanceof NoSuchFileException) {
                    handler.handle(Future.failedFuture(
                            ReturnHelper.returnFailure(404, "Sitemap index not found")));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, ar.cause().getMessage())));
                }
            }
        });
    }

    public void readSitemap(String sitemapId, Handler<AsyncResult<JsonObject>> handler) {
        vertx.fileSystem().readFile(readpath + "sitemap_datasets_" + sitemapId + ".xml", ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, ar.result().toString())));
            } else {
                if (ar.cause() instanceof FileSystemException && ar.cause().getCause() instanceof NoSuchFileException) {
                    handler.handle(Future.failedFuture(
                            ReturnHelper.returnFailure(404, "Sitemap not found")));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, ar.cause().getMessage())));
                }
            }
        });
    }

    public void triggerSitemapGeneration(Handler<AsyncResult<JsonObject>> handler)  {
        executor.executeBlocking(promise -> {
            generateSitemaps();
            promise.complete();
        }, res -> {});
        handler.handle(Future.succeededFuture(ReturnHelper
                .returnSuccess(200, "Triggered sitemap generation")));
    }

    private void generateSitemaps() {
        scrollIds(sitemapConfig.getInteger("size", 10000), ar -> {
            if (ar.succeeded()) {
                List<Future> writeFilePromises = new ArrayList<>();
                JsonArray entries = ar.result().getJsonObject("result").getJsonArray("results");

                int amount = entries.size();
                String sitemapIndex = generateSitemapIndex(amount);
                Promise writeSitemapIndex = Promise.promise();
                writeFilePromises.add(writeSitemapIndex.future());
                if (sitemapIndex != null && !sitemapIndex.isEmpty()) {
                    vertx.fileSystem().writeFile(writepath + "sitemap_index.xml",
                            Buffer.buffer(sitemapIndex.getBytes()), writeFileResult -> {
                                if (writeFileResult.succeeded()) {
                                    writeSitemapIndex.complete();
                                } else {
                                    LOG.error("Sitemap: " + writeFileResult.cause().getMessage());
                                    writeSitemapIndex.fail(writeFileResult.cause());
                                }
                            });
                } else {
                    writeSitemapIndex.fail(new NullPointerException(sitemapIndex));
                }

                for (int i = 0; i < amount; ++i) {
                    String sitemap = generateSitemap(entries.getJsonArray(i));
                    Promise writeSitemap = Promise.promise();
                    writeFilePromises.add(writeSitemap.future());
                    if (sitemap != null && !sitemap.isEmpty()) {
                        vertx.fileSystem().writeFile(writepath + "sitemap_datasets_" + (i+1) + ".xml",
                                Buffer.buffer(sitemap.getBytes()), writeFileResult -> {
                                    if (writeFileResult.succeeded()) {
                                        writeSitemap.complete();
                                    } else {
                                        LOG.error("Sitemap: " + writeFileResult.cause().getMessage());
                                        writeSitemap.fail(writeFileResult.cause());
                                    }
                                });
                    } else {
                        writeSitemap.fail(new NullPointerException(sitemap));
                    }
                }

                CompositeFuture.all(writeFilePromises).setHandler(
                        writeFilesResult -> {
                            readpath = writepath;
                            writepath = writepath.equals("./sitemaps/1/") ? "./sitemaps/2/" : "./sitemaps/1/";
                        });
            } else {
                ar.cause().printStackTrace();
                LOG.error(ar.cause().getMessage());
            }
        });
    }

    private String generateSitemapIndex(int count) {

        SitemapIndex sitemapIndex = new SitemapIndex();

        sitemapIndex.addSitemap(sitemapConfig.getString("drupal", ""));

        for (int i = 0; i < count; ++i) {
            sitemapIndex.addSitemap(sitemapConfig.getString("url", "") + "sitemap_datasets_" + (i + 1) + ".xml");
        }

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(SitemapIndex.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            // output pretty printed
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            StringWriter sw = new StringWriter();
            jaxbMarshaller.marshal(sitemapIndex, sw);
            // DEBUG: jaxbMarshaller.marshal(feed, System.out);

            return sw.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String generateSitemap(JsonArray entries) {

        Sitemap sitemap = new Sitemap();

        for(Object obj : entries) {
            JsonObject entry = (JsonObject) obj;

            String language = "en";

            sitemap.addSitemap(sitemapConfig.getString("url", "") + "datasets" + "/" +
                    entry.getString("id"), language, sitemapConfig.getJsonArray("languages", new JsonArray()));
        }

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(Sitemap.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            // output pretty printed
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            StringWriter sw = new StringWriter();
            jaxbMarshaller.marshal(sitemap, sw);
            // DEBUG: jaxbMarshaller.marshal(feed, System.out);

            return sw.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void scrollIds(Integer size, Handler<AsyncResult<JsonObject>> handler) {
        JsonArray ids = new JsonArray();

        JsonObject q = new JsonObject();
        q.put("size", size);
        q.put("filter", "dataset");
        q.put("aggregation", false);
        q.put("includes", new JsonArray().add("id"));
        q.put("scroll", true);

        Query query = Json.decodeValue(q.toString(), Query.class);

        LOG.info(query.toString());

        Integer maxResultWindow = indexManager.getMaxResultWindow().get("dataset");
        Map<String, Field> fields = indexManager.getFields().get("dataset");
        Map<String, ImmutablePair<String, String>> facets = indexManager.getFacets().get("dataset");
        Map<String, String> searchParams = indexManager.getSearchParams().get("dataset");

        SearchRequest searchRequest = SearchRequestHelper
                .buildSearchRequest(query, maxResultWindow, fields, facets, searchParams);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();

            JsonArray results = SearchResponseHelper.simpleProcessSearchResult(searchResponse);
            ids.add(results);

            while(!results.isEmpty()) {
                LOG.info(scrollId);

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueSeconds(60));

                SearchResponse scrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = scrollResponse.getScrollId();

                results = SearchResponseHelper.simpleProcessSearchResult(scrollResponse);
                if (!results.isEmpty()) ids.add(results);
            }

            handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, new JsonObject().put("results", ids))));
        } catch (Exception e) {
            LOG.error("Search: " + e);
            e.printStackTrace();
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
        }
    }

}
