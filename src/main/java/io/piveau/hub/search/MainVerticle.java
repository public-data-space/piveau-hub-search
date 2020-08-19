package io.piveau.hub.search;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.services.catalogues.CataloguesService;
import io.piveau.hub.search.services.catalogues.CataloguesServiceVerticle;
import io.piveau.hub.search.services.dataservices.DataServicesService;
import io.piveau.hub.search.services.dataservices.DataServicesServiceVerticle;
import io.piveau.hub.search.services.datasets.DatasetsService;
import io.piveau.hub.search.services.datasets.DatasetsServiceVerticle;
import io.piveau.hub.search.services.gazetteer.GazetteerService;
import io.piveau.hub.search.services.gazetteer.GazetteerServiceVerticle;
import io.piveau.hub.search.services.search.SearchService;
import io.piveau.hub.search.services.search.SearchServiceVerticle;
import io.piveau.hub.search.services.sitemaps.SitemapsService;
import io.piveau.hub.search.services.sitemaps.SitemapsServiceVerticle;
import io.piveau.hub.search.verticles.ShellVerticle;
import io.piveau.hub.search.handler.*;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.api.contract.RouterFactoryOptions;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.api.validation.ValidationException;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MainVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);

    private DatasetHandler datasetHandler;
    private CatalogueHandler catalogueHandler;
    // private DistributionHandler distributionHandler;
    private DataServiceHandler dataServiceHandler;
    private SearchHandler searchHandler;
    private ApiKeyHandler apiKeyHandler;
    private GazetteerHandler gazetteerHandler;
    private CkanHandler ckanHandler;
    private FeedHandler feedHandler;
    private SitemapHandler sitemapHandler;

    @Override
    public void start(Promise<Void> startPromise) {
        loadConfig().compose(this::bootstrapVerticles).compose(this::startServer).setHandler(handler -> {
            if (handler.succeeded()) {
                LOG.info("Successfully launched hub-search");
                startPromise.complete();
            } else {
                LOG.error("Failed to launch hub-search: " + handler.cause());
                startPromise.fail(handler.cause());
            }
        });
    }

    private Future<Void> startServer(JsonObject config) {
        Promise<Void> promise = Promise.promise();

        Integer service_port = config.getInteger(Constants.ENV_PIVEAU_HUB_SEARCH_SERVICE_PORT, 8080);
        String api_key = config.getString(Constants.ENV_PIVEAU_HUB_SEARCH_API_KEY);

        if(api_key == null) {
            LOG.error("No api_key specified");
            promise.fail("No api_key specified");
            return promise.future();
        }

        Set<String> allowedHeaders = new HashSet<>();
        allowedHeaders.add("x-requested-with");
        allowedHeaders.add("Access-Control-Allow-Origin");
        allowedHeaders.add("origin");
        allowedHeaders.add("Content-Type");
        allowedHeaders.add("accept");

        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);
        allowedMethods.add(HttpMethod.OPTIONS);
        allowedMethods.add(HttpMethod.DELETE);
        allowedMethods.add(HttpMethod.PATCH);
        allowedMethods.add(HttpMethod.PUT);

        OpenAPI3RouterFactory.create(vertx, "webroot/openapi.yaml", handler -> {
            if (handler.succeeded()) {
                OpenAPI3RouterFactory routerFactory = handler.result();
                RouterFactoryOptions options = new RouterFactoryOptions()
                        .setMountNotImplementedHandler(true)
                        .setRequireSecurityHandlers(true);

                routerFactory.setOptions(options);

                apiKeyHandler = new ApiKeyHandler(api_key);
                routerFactory.addSecurityHandler("ApiKeyAuth", apiKeyHandler::checkApiKey);

                routerFactory.addHandlerByOperationId("createDataset", datasetHandler::createDataset);
                routerFactory.addHandlerByOperationId("createOrUpdateDataset", datasetHandler::createOrUpdateDataset);
                routerFactory.addHandlerByOperationId("modifyDataset", datasetHandler::modifyDataset);
                routerFactory.addHandlerByOperationId("readDataset", datasetHandler::readDataset);
                routerFactory.addHandlerByOperationId("deleteDataset", datasetHandler::deleteDataset);
                routerFactory.addHandlerByOperationId("createDatasetBulk", datasetHandler::createDatasetBulk);
                routerFactory.addHandlerByOperationId("createOrUpdateDatasetBulk", datasetHandler::createOrUpdateDatasetBulk);

                routerFactory.addHandlerByOperationId("createCatalogue", catalogueHandler::createCatalogue);
                routerFactory.addHandlerByOperationId("createOrUpdateCatalogue", catalogueHandler::createOrUpdateCatalogue);
                routerFactory.addHandlerByOperationId("modifyCatalogue", catalogueHandler::modifyCatalogue);
                routerFactory.addHandlerByOperationId("readCatalogue", catalogueHandler::readCatalogue);
                routerFactory.addHandlerByOperationId("deleteCatalogue", catalogueHandler::deleteCatalogue);

                routerFactory.addHandlerByOperationId("createDataService", dataServiceHandler::createDataService);
                routerFactory.addHandlerByOperationId("createOrUpdateDataService", dataServiceHandler::createOrUpdateDataService);
                routerFactory.addHandlerByOperationId("modifyDataService", dataServiceHandler::modifyDataService);
                routerFactory.addHandlerByOperationId("readDataService", dataServiceHandler::readDataService);
                routerFactory.addHandlerByOperationId("deleteDataService", dataServiceHandler::deleteDataService);

                routerFactory.addHandlerByOperationId("searchGet", searchHandler::searchGet);
                routerFactory.addHandlerByOperationId("searchPost", searchHandler::searchPost);
                routerFactory.addHandlerByOperationId("searchAutocomplete", searchHandler::searchAutocomplete);
                routerFactory.addHandlerByOperationId("scrollGet", searchHandler::scrollGet);

                routerFactory.addHandlerByOperationId("gazetteerAutocomplete", gazetteerHandler::autocomplete);

                routerFactory.addHandlerByOperationId("ckanPackageSearch", ckanHandler::package_search);
                routerFactory.addHandlerByOperationId("ckanPackageShow", ckanHandler::package_show);

                routerFactory.addHandlerByOperationId("datasets.atom", feedHandler::atom);
                routerFactory.addHandlerByOperationId("datasets.rss", feedHandler::rss);

                routerFactory.addHandlerByOperationId("readSitemapIndex", sitemapHandler::readSitemapIndex);
                routerFactory.addHandlerByOperationId("readSitemap", sitemapHandler::readSitemap);

                Router router = routerFactory.getRouter();
                router.route().handler(StaticHandler.create());
                router.route().handler(
                        CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods)
                );
                router.errorHandler(400, context -> {
                    Throwable failure = context.failure();
                    if (failure instanceof ValidationException) {
                        LOG.debug(failure.getMessage());
                        context.response().putHeader("Content-Type", "application/json");
                        JsonObject response = new JsonObject();
                        response.put("status", "error");
                        response.put("message", failure.getMessage());
                        context.response().setStatusCode(400);
                        context.response().end(response.encode());
                    }
                });

                HttpServer server = vertx.createHttpServer(new HttpServerOptions().setPort(service_port));
                server.requestHandler(router).listen((ar) ->  {
                    if (ar.succeeded()) {
                        LOG.info("Successfully launched server on port [{}]", service_port);
                        promise.complete();
                    } else {
                        LOG.error("Failed to start server at [{}]: {}", service_port, handler.cause());
                        promise.fail(ar.cause());
                    }
                });
            } else {
                // Something went wrong during router factory initialization
                LOG.error("Failed to start server at [{}]: {}", service_port, handler.cause());
                promise.fail(handler.cause());
            }
        });

        return promise.future();
    }

    private Future<JsonObject> loadConfig() {
        Promise<JsonObject> promise = Promise.promise();

        ConfigStoreOptions envStoreOptions = new ConfigStoreOptions()
                .setType("env")
                .setConfig(new JsonObject().put("keys", new JsonArray()
                        .add(Constants.ENV_PIVEAU_HUB_SEARCH_SERVICE_PORT)
                        .add(Constants.ENV_PIVEAU_HUB_SEARCH_BOOST)
                        .add(Constants.ENV_PIVEAU_HUB_SEARCH_API_KEY)
                        .add(Constants.ENV_PIVEAU_HUB_SEARCH_ES_CONFIG)
                        .add(Constants.ENV_PIVEAU_HUB_SEARCH_CLI_CONFIG)
                        .add(Constants.ENV_PIVEAU_HUB_SEARCH_GAZETTEER_CONFIG)
                        .add(Constants.ENV_PIVEAU_HUB_SEARCH_SITEMAP_CONFIG)
                ));

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "conf/config.json"));

        ConfigRetriever.create(vertx, new ConfigRetrieverOptions()
                .addStore(fileStoreOptions)
                .addStore(envStoreOptions)).getConfig(handler -> {
            if (handler.succeeded()) {
                LOG.info(handler.result().encodePrettily());
                promise.complete(handler.result());
            } else {
                promise.fail(handler.cause());
            }
        });

        return promise.future();
    }

    private Future<JsonObject> bootstrapVerticles(JsonObject config) {
        Promise<JsonObject> promise = Promise.promise();

        Promise<String> shellPromise = Promise.promise();
        vertx.deployVerticle(ShellVerticle.class.getName(), new DeploymentOptions()
                .setConfig(config).setWorker(true)/*.setInstances(4)*/, shellPromise);

        Promise<String> gazetteerPromise = Promise.promise();
        vertx.deployVerticle(GazetteerServiceVerticle.class.getName(), new DeploymentOptions()
                .setConfig(config).setWorker(true)/*.setInstances(4)*/, gazetteerPromise);

        Promise<String> datasetsPromise = Promise.promise();
        vertx.deployVerticle(DatasetsServiceVerticle.class.getName(), new DeploymentOptions()
                .setConfig(config).setWorker(true)/*.setInstances(4)*/, datasetsPromise);

        Promise<String> cataloguesPromise = Promise.promise();
        vertx.deployVerticle(CataloguesServiceVerticle.class.getName(), new DeploymentOptions()
                .setConfig(config).setWorker(true)/*.setInstances(4)*/, cataloguesPromise);

        Promise<String> dataServicePromise = Promise.promise();
        vertx.deployVerticle(DataServicesServiceVerticle.class.getName(), new DeploymentOptions()
                .setConfig(config).setWorker(true)/*.setInstances(4)*/, dataServicePromise);

        Promise<String> searchServiceVerticle = Promise.promise();
        vertx.deployVerticle(SearchServiceVerticle.class.getName(), new DeploymentOptions()
                        .setConfig(config).setWorker(true)/*.setInstances(4)*/, searchServiceVerticle);

        Promise<String> sitemapsServiceVerticle = Promise.promise();
        vertx.deployVerticle(SitemapsServiceVerticle.class.getName(), new DeploymentOptions()
                        .setConfig(config).setWorker(true)/*.setInstances(4)*/, sitemapsServiceVerticle);

        CompositeFuture.all(Arrays.asList(shellPromise.future(), gazetteerPromise.future(), datasetsPromise.future(),
                cataloguesPromise.future(), dataServicePromise.future(), searchServiceVerticle.future(),
                sitemapsServiceVerticle.future())).setHandler(ar -> {
            if (ar.succeeded()) {
                datasetHandler = new DatasetHandler(vertx, DatasetsService.SERVICE_ADDRESS);
                catalogueHandler = new CatalogueHandler(vertx, CataloguesService.SERVICE_ADDRESS);
                // distributionHandler = new DistributionHandler(vertx, DocumentsService.SERVICE_ADDRESS);
                dataServiceHandler = new DataServiceHandler(vertx, DataServicesService.SERVICE_ADDRESS);
                gazetteerHandler = new GazetteerHandler(vertx, GazetteerService.SERVICE_ADDRESS);
                searchHandler = new SearchHandler(vertx, SearchService.SERVICE_ADDRESS);
                ckanHandler = new CkanHandler(vertx, SearchService.SERVICE_ADDRESS, DatasetsService.SERVICE_ADDRESS);
                feedHandler = new FeedHandler(config.getJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_FEED_CONFIG),
                        vertx, SearchService.SERVICE_ADDRESS);
                sitemapHandler = new SitemapHandler(vertx, SitemapsService.SERVICE_ADDRESS);
                promise.complete(config);
            } else {
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }
}
