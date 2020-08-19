package io.piveau.hub.search.IntegrationTest;

import io.piveau.hub.search.util.connector.DataServiceConnector;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Testing the DataServiceConnector")
@ExtendWith(VertxExtension.class)
class DataServiceConnectorIntegrationTest {

    private final Logger LOG = LoggerFactory.getLogger(DataServiceConnectorIntegrationTest.class);

    public static class FixedElasticsearchContainerWithFixedPort extends ElasticsearchContainer {
        FixedElasticsearchContainerWithFixedPort(String dockerImageName) {
            super(dockerImageName);
            super.addFixedExposedPort(9200, 9200);
        }
    }

    private static FixedElasticsearchContainerWithFixedPort container = new FixedElasticsearchContainerWithFixedPort(
            "docker.elastic.co/elasticsearch/elasticsearch:7.1.0");

    private DataServiceConnector dataServiceConnector;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(3);

        container.start();

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "conf/test/elasticsearch_config_standard.json"));

        ConfigRetriever.create(vertx, new ConfigRetrieverOptions()
                .addStore(fileStoreOptions)).getConfig(configResult -> {
            if (configResult.succeeded()) {
                DataServiceConnector.create(vertx, configResult.result(), datasetConnectorResult -> {
                    if (datasetConnectorResult.succeeded()) {
                        dataServiceConnector = datasetConnectorResult.result();
                        checkpoint.flag();
                    } else {
                        testContext.failNow(datasetConnectorResult.cause());
                    }
                });
                checkpoint.flag();
            } else {
                testContext.failNow(configResult.cause());
            }
        });
        checkpoint.flag();
    }

    @AfterEach
    void tearDown(Vertx vertx, VertxTestContext testContext) {
        container.close();
        testContext.completeNow();
    }

    @Test
    @DisplayName("Test createDataService")
    void testCreateDataService(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(5);
        vertx.fileSystem().readFile("test/examples/dataservice.json", fileRes -> {
            if (fileRes.succeeded()) {
                JsonObject payload = new JsonObject(fileRes.result());
                dataServiceConnector.createDataService(payload, createDataServiceResult -> {
                    if (createDataServiceResult.succeeded()) {
                        int createStatus = createDataServiceResult.result().getInteger("status");
                        JsonObject createResult = createDataServiceResult.result().getJsonObject("result");
                        assertEquals(201, createStatus);
                        boolean matchesUUID = createResult.toString()
                                .matches("\\{\"id\":\"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\"}");
                        assertTrue(matchesUUID);
                        dataServiceConnector.readDataService(createResult.getString("id"), readDataServiceResult -> {
                            if (readDataServiceResult.succeeded()) {
                                int readStatus = readDataServiceResult.result().getInteger("status");
                                JsonObject readResult = readDataServiceResult.result().getJsonObject("result");
                                assertEquals(200, readStatus);
                                assertEquals(payload.put("id", createResult.getString("id")), readResult);
                                checkpoint.flag();
                            } else {
                                readDataServiceResult.cause().printStackTrace();
                                testContext.failNow(readDataServiceResult.cause());
                            }
                        });
                        checkpoint.flag();
                    } else {
                        createDataServiceResult.cause().printStackTrace();
                        testContext.failNow(createDataServiceResult.cause());
                    }
                });
                checkpoint.flag();
            } else {
                fileRes.cause().printStackTrace();
                testContext.failNow(fileRes.cause());
            }
            checkpoint.flag();
        });
        checkpoint.flag();
    }

    @Test
    @DisplayName("Test createOrUpdateDataService")
    void testCreateOrUpdateDataService(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(5);
        vertx.fileSystem().readFile("test/examples/dataservice.json", fileRes -> {
            if (fileRes.succeeded()) {
                JsonObject payload = new JsonObject(fileRes.result());
                dataServiceConnector.createOrUpdateDataService("dataservice", payload, createDataServiceResult -> {
                    if (createDataServiceResult.succeeded()) {
                        int createStatus = createDataServiceResult.result().getInteger("status");
                        JsonObject createResult = createDataServiceResult.result().getJsonObject("result");
                        assertEquals(201, createStatus);
                        assertEquals(new JsonObject().put("id", "dataservice"), createResult);
                        dataServiceConnector.readDataService("dataservice", readDataServiceResult -> {
                            if (readDataServiceResult.succeeded()) {
                                int readStatus = readDataServiceResult.result().getInteger("status");
                                JsonObject readResult = readDataServiceResult.result().getJsonObject("result");
                                assertEquals(200, readStatus);
                                assertEquals(payload, readResult);
                                checkpoint.flag();
                            } else {
                                readDataServiceResult.cause().printStackTrace();
                                testContext.failNow(readDataServiceResult.cause());
                            }
                        });
                        checkpoint.flag();
                    } else {
                        createDataServiceResult.cause().printStackTrace();
                        testContext.failNow(createDataServiceResult.cause());
                    }
                });
                checkpoint.flag();
            } else {
                fileRes.cause().printStackTrace();
                testContext.failNow(fileRes.cause());
            }
            checkpoint.flag();
        });
        checkpoint.flag();
    }

    @Test
    @DisplayName("Test modifyDataService")
    void testModifyDataService(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(6);
        vertx.fileSystem().readFile("test/examples/dataservice.json", fileRes -> {
            if (fileRes.succeeded()) {
                JsonObject payload = new JsonObject(fileRes.result());
                dataServiceConnector.createOrUpdateDataService("dataservice", payload, createDataServiceResult -> {
                    if (createDataServiceResult.succeeded()) {
                        int createStatus = createDataServiceResult.result().getInteger("status");
                        JsonObject createResult = createDataServiceResult.result().getJsonObject("result");
                        assertEquals(201, createStatus);
                        assertEquals(new JsonObject().put("id", "dataservice"), createResult);
                        JsonObject toBePatched = new JsonObject().put("title", new JsonObject().put("en", "Modified Title"));
                        dataServiceConnector.modifyDataService("dataservice", toBePatched, modifyDataServiceResult -> {
                            if (modifyDataServiceResult.succeeded()) {
                                int modifyStatus = modifyDataServiceResult.result().getInteger("status");
                                JsonObject modifyResult = modifyDataServiceResult.result().getJsonObject("result");
                                assertEquals(200, modifyStatus);
                                assertEquals(new JsonObject().put("id", "dataservice"), modifyResult);
                                dataServiceConnector.readDataService("dataservice", readDataServiceResult -> {
                                    if (readDataServiceResult.succeeded()) {
                                        int readStatus = readDataServiceResult.result().getInteger("status");
                                        JsonObject readResult = readDataServiceResult.result().getJsonObject("result");
                                        assertEquals(200, readStatus);
                                        assertEquals(payload.put("title", toBePatched.getJsonObject("title")), readResult);
                                        checkpoint.flag();
                                    } else {
                                        readDataServiceResult.cause().printStackTrace();
                                        testContext.failNow(readDataServiceResult.cause());
                                    }
                                });
                                checkpoint.flag();
                            } else {
                                modifyDataServiceResult.cause().printStackTrace();
                                testContext.failNow(modifyDataServiceResult.cause());
                            }
                        });
                        checkpoint.flag();
                    } else {
                        createDataServiceResult.cause().printStackTrace();
                        testContext.failNow(createDataServiceResult.cause());
                    }
                });
                checkpoint.flag();
            } else {
                fileRes.cause().printStackTrace();
                testContext.failNow(fileRes.cause());
            }
            checkpoint.flag();
        });
        checkpoint.flag();
    }

    @Test
    @DisplayName("Test deleteDataService")
    void testDeleteDataService(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(6);
        vertx.fileSystem().readFile("test/examples/dataservice.json", fileRes -> {
            if (fileRes.succeeded()) {
                JsonObject payload = new JsonObject(fileRes.result());
                dataServiceConnector.createOrUpdateDataService("dataservice", payload, createDataServiceResult -> {
                    if (createDataServiceResult.succeeded()) {
                        int createStatus = createDataServiceResult.result().getInteger("status");
                        JsonObject createResult = createDataServiceResult.result().getJsonObject("result");
                        assertEquals(201, createStatus);
                        assertEquals(new JsonObject().put("id", "dataservice"), createResult);
                        dataServiceConnector.deleteDataService("dataservice", deleteDataServiceResult -> {
                            if (deleteDataServiceResult.succeeded()) {
                                int deleteStatus = deleteDataServiceResult.result().getInteger("status");
                                JsonObject deleteResult = deleteDataServiceResult.result().getJsonObject("result");
                                assertEquals(200, deleteStatus);
                                assertEquals(new JsonObject().put("id", "dataservice"), deleteResult);
                                dataServiceConnector.readDataService("dataservice", readDataServiceResult -> {
                                    if (readDataServiceResult.succeeded()) {
                                        int readStatus = readDataServiceResult.result().getInteger("status");
                                        assertEquals(404, readStatus);
                                        checkpoint.flag();
                                    } else {
                                        JsonObject result = new JsonObject(readDataServiceResult.cause().getMessage());
                                        int readStatus = result.getInteger("status");
                                        assertEquals(404, readStatus);
                                        checkpoint.flag();
                                    }
                                });
                                checkpoint.flag();
                            } else {
                                deleteDataServiceResult.cause().printStackTrace();
                                testContext.failNow(deleteDataServiceResult.cause());
                            }
                        });
                        checkpoint.flag();
                    } else {
                        createDataServiceResult.cause().printStackTrace();
                        testContext.failNow(createDataServiceResult.cause());
                    }
                });
                checkpoint.flag();
            } else {
                fileRes.cause().printStackTrace();
                testContext.failNow(fileRes.cause());
            }
            checkpoint.flag();
        });
        checkpoint.flag();
    }
}