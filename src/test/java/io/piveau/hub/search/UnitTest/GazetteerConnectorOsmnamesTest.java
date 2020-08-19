package io.piveau.hub.search.UnitTest;

import io.piveau.hub.search.util.gazetteer.GazetteerConnector;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Testing the GazetteerConnectorOsmnames")
@ExtendWith(VertxExtension.class)
class GazetteerConnectorOsmnamesTest {

    private final Logger LOG = LoggerFactory.getLogger(FeedHandlerTest.class);

    private GazetteerConnector gazetteerConnector;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "conf/test/gazetteer/osmnames_config.json"));

        WebClient client = WebClient.create(vertx);

        ConfigRetriever.create(vertx, new ConfigRetrieverOptions()
                .addStore(fileStoreOptions)).getConfig(configResult -> {
            if (configResult.succeeded()) {
                GazetteerConnector.create(client, configResult.result(), gazetteerConnectorResult -> {
                    if (gazetteerConnectorResult.succeeded()) {
                        gazetteerConnector = gazetteerConnectorResult.result();
                        checkpoint.flag();
                    } else {
                        testContext.failNow(gazetteerConnectorResult.cause());
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
    @DisplayName("Testing buildUrl")
    void testBuildUrl(Vertx vertx, VertxTestContext testContext) {
        String q = "test";
        String url = gazetteerConnector.buildUrl(q);
        assertEquals("http://localhost:8089/q/test.js", url);
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing autocomplete with null query")
    void testAutocompleteWithNullQuery(Vertx vertx, VertxTestContext testContext) {
        String q = null;
        gazetteerConnector.autocomplete(q, autocompleteResult -> {
            assertTrue(autocompleteResult.failed());
            assertEquals("{\"status\":404,\"message\":\"Query null or empty!\"}",
                    autocompleteResult.cause().getMessage());
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Testing autocomplete with empty query")
    void testAutocompleteWithEmptyQuery(Vertx vertx, VertxTestContext testContext) {
        String q = "";
        gazetteerConnector.autocomplete(q, autocompleteResult -> {
            assertTrue(autocompleteResult.failed());
            assertEquals("{\"status\":404,\"message\":\"Query null or empty!\"}",
                    autocompleteResult.cause().getMessage());
            testContext.completeNow();
        });
    }

    @Test
    @DisplayName("Testing querySuggestion with null input")
    void testQuerySuggestionWithNullInput(Vertx vertx, VertxTestContext testContext) {
        JsonObject testMessage = null;
        JsonObject result = gazetteerConnector.querySuggestion(testMessage);
        assertEquals(new JsonObject().put("results", new JsonArray()), result);
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing querySuggestion with null input")
    void testQuerySuggestionWithEmptyInput(Vertx vertx, VertxTestContext testContext) {
        JsonObject testMessage = new JsonObject();
        JsonObject result = gazetteerConnector.querySuggestion(testMessage);
        assertEquals(new JsonObject().put("results", new JsonArray()), result);
        testContext.completeNow();
    }

    @Test
    @DisplayName("querySuggestion with simple input")
    void testQuerySuggestionWithSimpleInput(Vertx vertx, VertxTestContext testContext) {
        JsonObject test = new JsonObject();
        test.put("display_name", "test");
        test.put("type", "state");
        test.put("boundingbox", new JsonArray().add(0.0).add(0.0).add(1.0).add(1.0));

        JsonArray testResults = new JsonArray();
        testResults.add(test);

        JsonObject testMessage = new JsonObject();
        testMessage.put("results", testResults);

        JsonObject result = gazetteerConnector.querySuggestion(testMessage);

        JsonObject control = new JsonObject();
        control.put("featureType", "state");
        control.put("name", "test");
        control.put("geometry", "0.0,0.0,1.0,1.0");

        JsonArray controlResults = new JsonArray();
        controlResults.add(control);

        JsonObject controlMessage = new JsonObject();
        controlMessage.put("results", controlResults);

        assertEquals(controlMessage, result);
        testContext.completeNow();
    }
}
