package io.piveau.hub.search.UnitTest;

import io.piveau.hub.search.util.search.SearchResponseHelper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Testing the SearchResponseHelper")
@ExtendWith(VertxExtension.class)
class SearchResponseHelperTest {

    private final Logger LOG = LoggerFactory.getLogger(SearchResponseHelperTest.class);

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        testContext.completeNow();
    }

    @AfterEach
    void tearDown(Vertx vertx, VertxTestContext testContext) {
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing basic processSearchResult with correct modification_date and release_date")
    void testProcessSearchResultWithCorrectDates(Vertx vertx, VertxTestContext testContext) {
        try {
            // TODO: Check how to set index correctly
            String modification_date = "_" + DateTime.now().plusYears(1).toString();
            String release_date = "_" + DateTime.now().plusYears(1).toString();

            SearchHit[] hits = new SearchHit[1];

            BytesReference source1 = new BytesArray("{\n" +
                    "                \"id\" : \"test-id-1\",\n" +
                    "                \"modification_date\" : \"" + modification_date +"\",\n" +
                    "                \"release_date\" : \"" + release_date +"\"\n" +
                    "            }");

            Map<String, DocumentField> fields = new HashMap<>();

            DocumentField ignored = new DocumentField("_ignored", new ArrayList<>());

            ignored.getValues().add("modification_date");
            ignored.getValues().add("release_date");

            fields.put("_ignored", ignored);

            hits[0] = new SearchHit(1, "test-id-1", new Text("_doc"), fields).sourceRef(source1);

            JsonArray results = SearchResponseHelper
                    .processSearchResult(hits,null, null, null, null, null);

            JsonObject result = results.getJsonObject(0);

            assertEquals(modification_date.substring(1), result.getString("modification_date"));
            assertEquals(release_date.substring(1), result.getString("release_date"));

            testContext.completeNow();
        } catch (Exception e) {
            testContext.failNow(e);
        }
    }

    @Test
    @DisplayName("Testing basic processSearchResult with modification_date and release_date in future")
    void testProcessSearchResultWithFutureDates(Vertx vertx, VertxTestContext testContext) {
        try {
            // TODO: Check how to set index correctly
            String modification_date = DateTime.now().toString();
            String release_date = DateTime.now().toString();

            SearchHit[] hits = new SearchHit[1];

            BytesReference source1 = new BytesArray("{\n" +
                    "                \"id\" : \"test-id-1\",\n" +
                    "                \"modification_date\" : \"" + modification_date +"\",\n" +
                    "                \"release_date\" : \"" + release_date +"\"\n" +
                    "            }");

            hits[0] = new SearchHit(1, "test-id-1", new Text("_doc"), null).sourceRef(source1);

            JsonArray results = SearchResponseHelper
                    .processSearchResult(hits,null, null, null, null, null);

            JsonObject result = results.getJsonObject(0);

            assertEquals(modification_date, result.getString("modification_date"));
            assertEquals(release_date, result.getString("release_date"));

            testContext.completeNow();
        } catch (Exception e) {
            testContext.failNow(e);
        }
    }

    @Test
    @DisplayName("Testing basic processSearchResult with malformed modification_date and release_date")
    void testProcessSearchResultWithMalformedDates(Vertx vertx, VertxTestContext testContext) {
        try {
            // TODO: Check how to set index correctly
            String modification_date = "malformed_modification_date";
            String release_date = "malformed_release_date";

            SearchHit[] hits = new SearchHit[1];

            BytesReference source1 = new BytesArray("{\n" +
                    "                \"id\" : \"test-id-1\",\n" +
                    "                \"modification_date\" : \"" + modification_date +"\",\n" +
                    "                \"release_date\" : \"" + release_date +"\"\n" +
                    "            }");

            Map<String, DocumentField> fields = new HashMap<>();

            DocumentField ignored = new DocumentField("_ignored", new ArrayList<>());

            ignored.getValues().add("modification_date");
            ignored.getValues().add("release_date");

            fields.put("_ignored", ignored);

            hits[0] = new SearchHit(1, "test-id-1", new Text("_doc"), fields).sourceRef(source1);

            JsonArray results = SearchResponseHelper
                    .processSearchResult(hits,null, null, null, null, null);

            JsonObject result = results.getJsonObject(0);

            assertEquals(modification_date, result.getString("modification_date"));
            assertEquals(release_date, result.getString("release_date"));

            testContext.completeNow();
        } catch (Exception e) {
            testContext.failNow(e);
        }
    }
}