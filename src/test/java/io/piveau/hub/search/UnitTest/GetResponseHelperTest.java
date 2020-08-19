package io.piveau.hub.search.UnitTest;

import io.piveau.hub.search.util.response.GetResponseHelper;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.get.GetResult;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Testing the GetReponseHelper")
@ExtendWith(VertxExtension.class)
class GetResponseHelperTest {

    private final Logger LOG = LoggerFactory.getLogger(GetResponseHelperTest.class);

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        testContext.completeNow();
    }

    @AfterEach
    void tearDown(Vertx vertx, VertxTestContext testContext) {
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing basic getResponseToJson with modification_date and release_date")
    void testGetResponseToJsonWithDates(Vertx vertx, VertxTestContext testContext) {
        try {
            String modification_date = DateTime.now().toString();
            String release_date = DateTime.now().toString();

            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("id", "test-id")
                    .field("modification_date", modification_date)
                    .field("release_date", release_date)
                    .endObject();

            GetResult getResult = new GetResult(
                    "dataset",
                    "_doc",
                    "test-id",
                    1,
                    1,
                    1,
                    true,
                    BytesReference.bytes(builder),
                    null
            );

            GetResponse getResponse = new GetResponse(getResult);

            JsonObject result = GetResponseHelper.getResponseToJson(getResponse, null);

            assertEquals(modification_date, result.getString("modification_date"));
            assertEquals(release_date, result.getString("release_date"));

            testContext.completeNow();
        } catch (IOException e) {
            testContext.failNow(e);
        }
    }

    @Test
    @DisplayName("Testing basic getResponseToJson with modification_date and release_date in future")
    void testGetResponseToJsonWithFutureDates(Vertx vertx, VertxTestContext testContext) {
        try {
            String modification_date = "_" + DateTime.now().plusYears(1).toString();
            String release_date = "_" + DateTime.now().plusYears(1).toString();

            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("id", "test-id")
                    .field("modification_date", modification_date)
                    .field("release_date", release_date)
                    .endObject();

            DocumentField ignored = new DocumentField("_ignored", new ArrayList<>());

            ignored.getValues().add("modification_date");
            ignored.getValues().add("release_date");

            Map<String, DocumentField> fields = new HashMap<>();
            fields.put("_ignored", ignored);

            GetResult getResult = new GetResult(
                    "dataset",
                    "_doc",
                    "test-id",
                    1,
                    1,
                    1,
                    true,
                    BytesReference.bytes(builder),
                    fields
            );

            GetResponse getResponse = new GetResponse(getResult);

            JsonObject result = GetResponseHelper.getResponseToJson(getResponse, null);

            assertEquals(modification_date.substring(1), result.getString("modification_date"));
            assertEquals(release_date.substring(1), result.getString("release_date"));

            testContext.completeNow();
        } catch (IOException e) {
            testContext.failNow(e);
        }
    }

    @Test
    @DisplayName("Testing basic getResponseToJson with malformed modification_date and release_date")
    void testGetResponseToJsonWithMalformedDates(Vertx vertx, VertxTestContext testContext) {
        try {
            String modification_date = "malformed_modification_date";
            String release_date = "malformed_release_date";

            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("id", "test-id")
                    .field("modification_date", modification_date)
                    .field("release_date", release_date)
                    .endObject();

            DocumentField ignored = new DocumentField("_ignored", new ArrayList<>());

            ignored.getValues().add("modification_date");
            ignored.getValues().add("release_date");

            Map<String, DocumentField> fields = new HashMap<>();
            fields.put("_ignored", ignored);

            GetResult getResult = new GetResult(
                    "dataset",
                    "_doc",
                    "test-id",
                    1,
                    1,
                    1,
                    true,
                    BytesReference.bytes(builder),
                    fields
            );

            GetResponse getResponse = new GetResponse(getResult);

            JsonObject result = GetResponseHelper.getResponseToJson(getResponse, null);

            assertEquals(modification_date, result.getString("modification_date"));
            assertEquals(release_date, result.getString("release_date"));

            testContext.completeNow();
        } catch (IOException e) {
            testContext.failNow(e);
        }
    }

}