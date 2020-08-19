package io.piveau.hub.search.UnitTest;

import io.piveau.hub.search.util.gazetteer.GazetteerConnector;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
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

@DisplayName("Testing the GazetteerConnectorConterra")
@ExtendWith(VertxExtension.class)
class GazetteerConnectorConterraTest {

    private final Logger LOG = LoggerFactory.getLogger(FeedHandlerTest.class);

    private GazetteerConnector gazetteerConnector;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "conf/test/gazetteer/conterra_config.json"));

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
        assertEquals("http://localhost:8089/gazetteer/gazetteer/query?q=test", url);
        testContext.completeNow();
    }

}
