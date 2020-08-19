package io.piveau.hub.search.UnitTest;

import io.piveau.hub.search.util.connector.DatasetConnector;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Testing the DatasetConnector")
@ExtendWith(VertxExtension.class)
class DatasetConnectorTest {

    private final Logger LOG = LoggerFactory.getLogger(DatasetConnectorTest.class);

    private DatasetConnector datasetConnector;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(3);

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "conf/test/elasticsearch_config_standard.json"));

        ConfigRetriever.create(vertx, new ConfigRetrieverOptions()
                .addStore(fileStoreOptions)).getConfig(configResult -> {
            if (configResult.succeeded()) {
                // LOG.debug(configResult.result().encodePrettily());
                DatasetConnector.create(vertx, configResult.result(), datasetConnectorResult -> {
                    if (datasetConnectorResult.succeeded()) {
                        datasetConnector = datasetConnectorResult.result();
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
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing checkDates with correct payload")
    void testCheckDatesWithCorrectPayload(Vertx vertx, VertxTestContext testContext) {
        JsonObject payload_before = new JsonObject()
                .put("id", "test-id")
                .put("modification_date", new DateTime().toString())
                .put("release_date", new DateTime().toString());

        JsonObject payload_after = new JsonObject(payload_before.toString());

        datasetConnector.checkDates(payload_after, checkDatesResult -> {
            assertEquals(payload_before, payload_after);
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Testing checkDates with modification_date in future")
    void testCheckDatesFutureModificationDate(Vertx vertx, VertxTestContext testContext) {
        JsonObject payload_before = new JsonObject()
                .put("id", "test-id")
                .put("modification_date", new DateTime().plusYears(1).toString())
                .put("release_date", new DateTime().toString());

        JsonObject payload_after = new JsonObject(payload_before.toString());

        datasetConnector.checkDates(payload_after, checkDatesResult -> {
            assertEquals("_" + payload_before.getString("modification_date"),
                    payload_after.getString("modification_date"));
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Testing checkDates with release_date in future")
    void testCheckDatesFutureReleaseDate(Vertx vertx, VertxTestContext testContext) {
        JsonObject payload_before = new JsonObject()
                .put("id", "test-id")
                .put("modification_date", new DateTime().toString())
                .put("release_date", new DateTime().plusYears(1).toString());

        JsonObject payload_after = new JsonObject(payload_before.toString());

        datasetConnector.checkDates(payload_after, checkDatesResult -> {
            assertEquals("_" + payload_before.getString("release_date"),
                    payload_after.getString("release_date"));
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Testing checkDates with modification_date and release_date in future")
    void testCheckDatesFutureModificationDateAndFutureReleaseDate(Vertx vertx, VertxTestContext testContext) {
        JsonObject payload_before = new JsonObject()
                .put("id", "test-id")
                .put("modification_date", new DateTime().plusYears(1).toString())
                .put("release_date", new DateTime().plusYears(1).toString());

        JsonObject payload_after = new JsonObject(payload_before.toString());

        datasetConnector.checkDates(payload_after, checkDatesResult -> {
            assertEquals("_" + payload_before.getString("modification_date"),
                    payload_after.getString("modification_date"));
            assertEquals("_" + payload_before.getString("release_date"),
                    payload_after.getString("release_date"));
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Testing checkDates with correct payload array")
    void testCheckDatesWithCorrectPayloadArray(Vertx vertx, VertxTestContext testContext) {
        JsonArray payload_array_before = new JsonArray()
                .add(new JsonObject()
                        .put("id", "test-id-1")
                        .put("modification_date", new DateTime().toString())
                        .put("release_date", new DateTime().toString()))
                .add(new JsonObject()
                        .put("id", "test-id-2")
                        .put("modification_date", new DateTime().toString())
                        .put("release_date", new DateTime().toString()));

        JsonArray payload_array_after = new JsonArray(payload_array_before.toString());

        datasetConnector.checkDates(payload_array_after, checkDatesResult -> {
            assertEquals(payload_array_before, payload_array_after);
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Testing checkDates with permutating modification_date and release_date in future")
    void testCheckDatesWithPermutatingPayloadArray(Vertx vertx, VertxTestContext testContext) {
        JsonArray payload_array_before = new JsonArray()
                .add(new JsonObject()
                        .put("id", "test-id-1")
                        .put("modification_date", new DateTime().toString())
                        .put("release_date", new DateTime().toString()))
                .add(new JsonObject()
                        .put("id", "test-id-2")
                        .put("modification_date", new DateTime().plusYears(1).toString())
                        .put("release_date", new DateTime().toString()))
                .add(new JsonObject()
                        .put("id", "test-id-3")
                        .put("modification_date", new DateTime().toString())
                        .put("release_date", new DateTime().plusYears(1).toString()))
                .add(new JsonObject()
                        .put("id", "test-id-4")
                        .put("modification_date", new DateTime().plusYears(1).toString())
                        .put("release_date", new DateTime().plusYears(1).toString()));

        JsonArray payload_array_after = new JsonArray(payload_array_before.toString());

        datasetConnector.checkDates(payload_array_after, checkDatesResult -> {
            for (int i = 0; i < 4; ++i) {
                if ((i & 1) == 1) {
                    assertEquals("_" + payload_array_before.getJsonObject(i).getString("modification_date"),
                            payload_array_after.getJsonObject(i).getString("modification_date"));
                } else {
                    assertEquals(payload_array_before.getJsonObject(i).getString("modification_date"),
                            payload_array_after.getJsonObject(i).getString("modification_date"));
                }

                if ((i & 2) == 2) {
                    assertEquals("_" + payload_array_before.getJsonObject(i).getString("release_date"),
                            payload_array_after.getJsonObject(i).getString("release_date"));
                } else {
                    assertEquals(payload_array_before.getJsonObject(i).getString("release_date"),
                            payload_array_after.getJsonObject(i).getString("release_date"));
                }
            }

            testContext.completeNow();
        });
    }

}