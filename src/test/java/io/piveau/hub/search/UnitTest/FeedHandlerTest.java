package io.piveau.hub.search.UnitTest;

import io.piveau.hub.search.handler.FeedHandler;
import io.piveau.hub.search.services.search.SearchService;
import io.piveau.hub.search.util.feed.atom.AtomFeed;
import io.piveau.hub.search.util.feed.rss.RSSFeed;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@DisplayName("Testing the FeedHandler")
@ExtendWith(VertxExtension.class)
class FeedHandlerTest {

    private final Logger LOG = LoggerFactory.getLogger(FeedHandlerTest.class);

    private FeedHandler feedHandler;
    private JsonObject config;

    @BeforeEach
    void setUp(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "conf/test/feed_config_standard.json"));

        ConfigRetriever.create(vertx, new ConfigRetrieverOptions()
                .addStore(fileStoreOptions)).getConfig(configResult -> {
            if (configResult.succeeded()) {
                config = configResult.result();
                feedHandler = new FeedHandler(configResult.result(), vertx, SearchService.SERVICE_ADDRESS);
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
    @DisplayName("Testing checkLang with empty dataset")
    void testCheckLangEmptyDataset(Vertx vertx, VertxTestContext testContext) {
        JsonObject dataset = new JsonObject();
        String result = feedHandler.checkLang(dataset, "en");
        assertNull(result);
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing checkLang with empty translation_meta")
    void testCheckLangEmptyTranslationMeta(Vertx vertx, VertxTestContext testContext) {
        JsonObject dataset = new JsonObject();
        dataset.put("translation_meta", new JsonObject());
        String result = feedHandler.checkLang(dataset, "en");
        assertNull(result);
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing checkLang with empty full_available_languages")
    void testCheckLangEmptyFullAvailableLanguages(Vertx vertx, VertxTestContext testContext) {
        JsonArray full_available_languages = new JsonArray();
        JsonObject dataset = new JsonObject();
        dataset.put("translation_meta", new JsonObject().put("full_available_languages", full_available_languages));
        String result = feedHandler.checkLang(dataset, "en");
        assertNull(result);
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing checkLang with not available language")
    void testCheckLangNotAvailableLanguage(Vertx vertx, VertxTestContext testContext) {
        JsonArray full_available_languages = new JsonArray().add("de");
        JsonObject dataset = new JsonObject();
        dataset.put("translation_meta", new JsonObject().put("full_available_languages", full_available_languages));
        String result = feedHandler.checkLang(dataset, "en");
        assertEquals("de", result);
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing checkLang with not available language")
    void testCheckLangAvailableFullAvailLanguage(Vertx vertx, VertxTestContext testContext) {
        JsonArray full_available_languages = new JsonArray().add("de").add("en");
        JsonObject dataset = new JsonObject();
        dataset.put("translation_meta", new JsonObject().put("full_available_languages", full_available_languages));
        String result = feedHandler.checkLang(dataset, "en");
        assertEquals("en", result);
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing basic generate rss functionality")
    void testGenerateRSSFeed(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "src/test/resources/example_search_response.json"));

        ConfigRetriever.create(vertx, new ConfigRetrieverOptions()
                .addStore(fileStoreOptions)).getConfig(configResult -> {
            if (configResult.succeeded()) {
                String[] lang_array = {"en", "es", "fr"};
                for (String lang : lang_array) {

                    JsonObject result = configResult.result().getJsonObject("result");
                    String rssFeed = feedHandler.generateRSSFeed(result, "http://localhost:8080",
                            "http://localhost:8080/" + lang + "/feeds/datasets.rss",
                            "/" + lang + "/feeds/datasets.rss", lang);

                    RSSFeed feed = new RSSFeed();
                    try {
                        JAXBContext jaxbContext = JAXBContext.newInstance(RSSFeed.class);
                        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
                        feed = (RSSFeed) jaxbUnmarshaller.unmarshal(new StringReader(rssFeed));
                    } catch (JAXBException e) {
                        testContext.failNow(e);
                    }

                    assertEquals("2.0", feed.getVersion());

                    RSSFeed.Channel channel = feed.getChannel();
                    assertEquals(config.getString("title") + " - RSS Feed", channel.getTitle());
                    assertEquals("", channel.getCopyright());
                    assertEquals("", channel.getDescription());
                    assertEquals(lang, channel.getLanguage());
                    assertEquals("http://localhost:8080" + config.getString("relative_path_search") +
                            "/search?filter=dataset", channel.getLink());

                    for (int i = 0; i < channel.getItems().size(); ++i) {
                        RSSFeed.Item item = channel.getItems().get(i);
                        JsonObject dataset = result.getJsonArray("results").getJsonObject(i);

                        String avail_lang = feedHandler.checkLang(dataset, lang);

                        assertEquals("http://localhost:8080" + config.getString("relative_path_datasets") +
                                dataset.getString("id"), item.getGuid());
                        assertEquals("http://localhost:8080" + config.getString("relative_path_datasets") +
                                dataset.getString("id"), item.getLink());
                        assertEquals(dataset.getJsonObject("title").getString(avail_lang), item.getTitle());
                        assertEquals(dataset.getJsonObject("description").getString(avail_lang), item.getDescription());
                        assertEquals(dataset.getString("modification_date"), item.getPubDate());
                    }
                }
                checkpoint.flag();
            } else {
                testContext.failNow(configResult.cause());
            }
        });
        checkpoint.flag();
        testContext.completeNow();
    }

    @Test
    @DisplayName("Testing basic generate atom functionality")
    void testGenerateAtomFeed(Vertx vertx, VertxTestContext testContext) {
        Checkpoint checkpoint = testContext.checkpoint(2);

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "src/test/resources/example_search_response.json"));

        ConfigRetriever.create(vertx, new ConfigRetrieverOptions()
                .addStore(fileStoreOptions)).getConfig(configResult -> {
            if (configResult.succeeded()) {
                String[] lang_array = {"en", "es", "fr"};
                for (String lang : lang_array) {
                    JsonObject result = configResult.result().getJsonObject("result");
                    String atomFeed = feedHandler.generateAtomFeed(result, "http://localhost:8080",
                            "http://localhost:8080/" + lang + "/feeds/datasets.atom",
                            "/" + lang + "/feeds/datasets.atom", 0, 10, lang);

                    AtomFeed feed = new AtomFeed();
                    try {
                        JAXBContext jaxbContext = JAXBContext.newInstance(AtomFeed.class);
                        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
                        feed = (AtomFeed) jaxbUnmarshaller.unmarshal(new StringReader(atomFeed));
                    } catch (JAXBException e) {
                        testContext.failNow(e);
                    }

                    // TODO: test links, something wrong with unmarshalling here

                    assertEquals(lang, feed.getXml_lang());
                    assertEquals(config.getString("title") + " - Atom Feed", feed.getTitle());
                    assertEquals("http://localhost:8080" + config.getString("relative_path_search") +
                            "/" + lang + "/feeds/datasets.atom", feed.getId());

                    AtomFeed.Author author = feed.getAuthor();
                    assertEquals(config.getString("title"), author.getName());
                    assertEquals("http://localhost:8080", author.getUri());

                    assertEquals("", feed.getSubtitle());

                    for (int i = 0; i < feed.getEntries().size(); ++i) {
                        AtomFeed.Entry entry = feed.getEntries().get(i);
                        JsonObject dataset = result.getJsonArray("results").getJsonObject(i);

                        String avail_lang = feedHandler.checkLang(dataset, lang);

                        assertEquals("http://localhost:8080" + config.getString("relative_path_datasets") +
                                dataset.getString("id"), entry.getId());
                        assertEquals(dataset.getJsonObject("title").getString(avail_lang), entry.getTitle());
                        assertEquals(dataset.getJsonObject("description").getString(avail_lang), entry.getSummary().getContent());
                        assertEquals(dataset.getString("modification_date"), entry.getUpdated());
                    }
                }
                checkpoint.flag();
            } else {
                testContext.failNow(configResult.cause());
            }
        });
        checkpoint.flag();
        testContext.completeNow();
    }
}