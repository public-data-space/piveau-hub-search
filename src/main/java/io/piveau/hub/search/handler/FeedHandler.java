package io.piveau.hub.search.handler;

import io.piveau.hub.search.util.feed.atom.AtomFeed;
import io.piveau.hub.search.util.feed.rss.RSSFeed;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;

public class FeedHandler extends SearchHandler {

    private JsonObject config;

    public FeedHandler(JsonObject config, Vertx vertx, String address) {
        super(vertx, address);
        this.config = config;
    }

    public String checkLang(JsonObject dataset, String lang) {
        JsonObject translation_meta = dataset.getJsonObject("translation_meta");
        if (translation_meta == null) {
            return null;
        } else {
            JsonArray full_available_languages =  translation_meta.getJsonArray("full_available_languages");
            if (full_available_languages == null || full_available_languages.isEmpty()) {
                return null;
            } else {
                if (full_available_languages.contains(lang)) {
                    return lang;
                } else {
                    return full_available_languages.getString(0);
                }
            }
        }
    }

    public String generateAtomFeed(JsonObject result, String uri, String absoluteUri, String path, Integer pageInt,
                                   Integer limitInt, String lang) {
        Integer count = result.getInteger("count");

        Integer last = count % limitInt == 0 ? Math.max(count / limitInt - 1, 0) : Math.max(count / limitInt, 0);

        JsonObject links = new JsonObject();
        String feedId = "";

        String questionMark = absoluteUri.contains("?") ? "" : "?";
        String questionMarkOrAnd = absoluteUri.contains("?") ? "&" : "?";
        boolean containsPage = absoluteUri.contains("page=");
        boolean endsWithAnd = absoluteUri.endsWith("&");

        if (config.getString("relative_path_search") != null
                && !config.getString("relative_path_search").isEmpty()) {
            absoluteUri = absoluteUri.replace(path, config.getString("relative_path_search") + path);
            feedId = uri + config.getString("relative_path_search") + path;
        }

        links.put("self", absoluteUri);

        if (endsWithAnd) {
            links.put("alternate", absoluteUri.replace(path, "/search") + questionMark + "filter=dataset");
        } else {
            links.put("alternate", absoluteUri.replace(path, "/search") + questionMarkOrAnd + "filter=dataset");
        }

        links.put("first", absoluteUri.replaceAll("page=[0-9]+", "page=0"));

        if (pageInt != 0) {
            links.put("previous", absoluteUri.replaceAll("page=[0-9]+", "page=" + (pageInt - 1)));
        }

        if (pageInt < last) {
            if(containsPage) {
                links.put("next", absoluteUri.replaceAll("page=[0-9]+", "page=" + (pageInt + 1)));
            } else {
                if (endsWithAnd) {
                    links.put("next", absoluteUri.concat(questionMark + "page=2"));
                } else {
                    links.put("next", absoluteUri.concat(questionMarkOrAnd + "page=2"));
                }
            }
        }

        if(containsPage) {
            links.put("last", absoluteUri.replaceAll("page=[0-9]+", "page=" + last));
        } else {
            if (endsWithAnd) {
                links.put("last", absoluteUri.concat(questionMark + "page=" + last));
            } else {
                links.put("last", absoluteUri.concat(questionMarkOrAnd + "page=" + last));
            }
        }

        AtomFeed atomFeed = new AtomFeed(
                lang,
                config.getString("title"),
                feedId,
                config.getString("title"),
                uri,
                "",
                links
        );

        result.getJsonArray("results").forEach(dataset -> {
            JsonObject datasetJson = new JsonObject(dataset.toString());

            String dataset_lang = checkLang(datasetJson, lang);

            if (dataset_lang != null) {
                String id = datasetJson.getString("id");

                JsonObject title = datasetJson.getJsonObject("title");
                JsonObject description = datasetJson.getJsonObject("description");

                String title_lang = title != null ? title.getString(dataset_lang) : "n/a";
                String description_lang = description != null ? description.getString(dataset_lang) : "n/a";

                AtomFeed.Entry entry = atomFeed.addEntry(
                        dataset_lang,
                        uri + config.getString("relative_path_datasets") + id,
                        title_lang,
                        description_lang,
                        "html",
                        datasetJson.getString("release_date"),
                        datasetJson.getString("modification_date"),
                        uri + config.getString("relative_path_datasets") + id
                );

                JsonArray distributions = datasetJson.getJsonArray("distributions");
                if (distributions != null) {
                    distributions.forEach(dist -> {
                        JsonObject distJson = new JsonObject(dist.toString());

                        JsonObject format = distJson.getJsonObject("format");
                        String access_url = distJson.getString("access_url");

                        if (format != null && access_url != null && format.getString("id") != null &&
                                !format.getString("id").isEmpty() && !access_url.isEmpty()) {

                            entry.addLink(access_url, "enclosure", format.getString("id"));
                        }
                    });
                }
            }
        });

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(AtomFeed.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            // output pretty printed
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            StringWriter sw = new StringWriter();
            jaxbMarshaller.marshal(atomFeed, sw);
            // DEBUG: jaxbMarshaller.marshal(feed, System.out);

            return sw.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String generateRSSFeed(JsonObject result, String uri, String absoluteUri, String path, String lang) {
        String questionMarkOrAnd = absoluteUri.contains("?") ? "&" : "?";

        String link = absoluteUri.replace(path, config.getString("relative_path_search") + "/search");
        link += questionMarkOrAnd + "filter=dataset";
        link = link.replaceAll("&&", "&");
        link = link.replaceAll("\\?&", "?");

        RSSFeed rssFeed = new RSSFeed(
                config.getString("title"),
                link,
                "",
                lang,
                ""
        );

        result.getJsonArray("results").forEach(dataset -> {
            JsonObject datasetJson = new JsonObject(dataset.toString());

            String dataset_lang = checkLang(datasetJson, lang);

            if (dataset_lang != null) {
                String id = datasetJson.getString("id");

                JsonObject title = datasetJson.getJsonObject("title");
                JsonObject description = datasetJson.getJsonObject("description");

                String title_lang = title != null ? title.getString(dataset_lang) : "n/a";
                String description_lang = description != null ? description.getString(dataset_lang) : "n/a";

                // RSS has no field for modification date, we use pubdate instead.
                RSSFeed.Item item = rssFeed.addItem(
                        uri + config.getString("relative_path_datasets") + id,
                        title_lang,
                        uri + config.getString("relative_path_datasets") + id,
                        description_lang,
                        datasetJson.getString("modification_date")
                );

                JsonArray distributions = datasetJson.getJsonArray("distributions");
                if (distributions != null) {
                    distributions.forEach(dist -> {
                        JsonObject distJson = new JsonObject(dist.toString());

                        JsonObject format = distJson.getJsonObject("format");
                        String access_url = distJson.getString("access_url");

                        if (format != null && access_url != null && format.getString("id") != null &&
                                !format.getString("id").isEmpty() && !access_url.isEmpty()) {

                            item.addEnclosure(access_url, format.getString("id"));
                        }
                    });
                }
            }
        });

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(RSSFeed.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            // output pretty printed
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            StringWriter sw = new StringWriter();
            jaxbMarshaller.marshal(rssFeed, sw);

            return sw.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void feed(RoutingContext context, String type) {
        MultiMap params = context.request().params();

        JsonObject query = paramsToQuery(params);

        query.put("filter", "dataset");
        query.put("aggregation", false);

        if (query.getJsonArray("sort") == null || query.getJsonArray("sort").isEmpty()) {
            query.put("sort", new JsonArray().add("modification_date+desc"));
        }

        String lang = params.get("lang");

        String q = params.get("q");
        if (q != null)
            query.put("q", q);

        String uri = context.request().absoluteURI().replace(context.request().uri(), "");
        String absoluteUri = context.request().absoluteURI();
        String path = context.request().path();

        context.response().putHeader("Access-Control-Allow-Origin", "*");
        searchService.search(query.toString(), ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result().getJsonObject("result");

                int from = query.getInteger("from", 0);
                int size = query.getInteger("size", 10);

                String feed = "";

                if(type.equals("atom")) {
                    feed = generateAtomFeed(result, uri, absoluteUri, path, from/size, size, lang);
                }

                if(type.equals("rss")) {
                    feed = generateRSSFeed(result, uri, absoluteUri, path, lang);
                }

                if (feed != null && !feed.isEmpty()) {
                    if(type.equals("atom"))
                        context.response().putHeader("Content-Type", "application/atom+xml");
                    if(type.equals("rss"))
                        context.response().putHeader("Content-Type", "application/rss+xml");
                    context.response().setStatusCode(200);
                    context.response().end(feed);
                } else {
                    JsonObject response = new JsonObject();
                    response.put("success", false);
                    response.put("message", "Feed marshal error.");
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().setStatusCode(500);
                    context.response().end(response.toString());
                }
            } else {
                JsonObject response = new JsonObject();
                response.put("success", false);
                JsonObject result = new JsonObject(ar.cause().getMessage());
                response.put("result", result);
                Integer status = (Integer) result.remove("status");
                context.response().putHeader("Content-Type", "application/json");
                context.response().setStatusCode(status);
                context.response().end(response.toString());
            }
        });
    }

    public void atom(RoutingContext context) {
        feed(context, "atom");
    }

    public void rss(RoutingContext context) {
        feed(context, "rss");
    }
}
