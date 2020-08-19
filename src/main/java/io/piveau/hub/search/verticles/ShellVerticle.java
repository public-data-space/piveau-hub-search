package io.piveau.hub.search.verticles;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.services.datasets.DatasetsService;
import io.piveau.hub.search.services.catalogues.CataloguesService;
import io.piveau.hub.search.services.search.SearchService;
import io.piveau.hub.search.services.sitemaps.SitemapsService;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.shell.ShellService;
import io.vertx.ext.shell.ShellServiceOptions;
import io.vertx.ext.shell.command.CommandBuilder;
import io.vertx.ext.shell.command.CommandProcess;
import io.vertx.ext.shell.command.CommandRegistry;
import io.vertx.ext.shell.term.HttpTermOptions;
import io.vertx.ext.shell.term.TelnetTermOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ShellVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(ShellVerticle.class);

    private SearchService searchService;
    private DatasetsService datasetsService;
    private CataloguesService cataloguesService;
    private SitemapsService sitemapsService;

    @Override
    public void start(Promise<Void> startPromise) {
        searchService = SearchService.createProxy(vertx, SearchService.SERVICE_ADDRESS);
        datasetsService = DatasetsService.createProxy(vertx, DatasetsService.SERVICE_ADDRESS);
        cataloguesService = CataloguesService.createProxy(vertx, CataloguesService.SERVICE_ADDRESS);
        sitemapsService = SitemapsService.createProxy(vertx, SitemapsService.SERVICE_ADDRESS);

        JsonObject cli_config = ConfigHelper.forConfig(config())
                .forceJsonObject(Constants.ENV_PIVEAU_HUB_SEARCH_CLI_CONFIG);

        Integer cli_port = cli_config.getInteger("port", 8081);
        String cli_type = cli_config.getString("type", "http");

        ShellServiceOptions shellServiceOptions =
                new ShellServiceOptions().setWelcomeMessage("\n  Welcome to piveau-hub-search CLI!\n\n");

        ShellService shellService = null;

        if (cli_type.equals("http")) {
            shellService = ShellService.create(vertx,
                    shellServiceOptions.setHttpOptions(
                            new HttpTermOptions()
                                    .setHost("0.0.0.0")
                                    .setPort(cli_port)
                    )
            );
        }
        /*if (cli_type.equals("ssh")) {
            shellService = ShellService.create(vertx,
                    shellServiceOptions.setSSHOptions(
                            new SSHTermOptions().
                                    setHost("0.0.0.0").
                                    setPort(cli_port).
                                    setKeyPairOptions(new JksOptions().
                                            setPath("conf/shell/ssh.jks").
                                            setPassword("secret")
                                    ).
                                    setAuthOptions(new ShiroAuthOptions().
                                            setType(ShiroAuthRealmType.PROPERTIES).
                                            setConfig(new JsonObject().
                                                    put("properties_path", "conf/shell/auth.properties"))
                                    )
                    )
            );
        }*/
        if (cli_type.equals("telnet")) {
            shellService = ShellService.create(vertx,
                    shellServiceOptions.setTelnetOptions(
                            new TelnetTermOptions().
                                    setHost("0.0.0.0").
                                    setPort(cli_port)
                    )
            );
        }

        if (shellService != null) {
            shellService.start(handler -> {
                if (handler.succeeded()) {
                    LOG.info("Successfully launched cli on port [{}]", cli_port);
                    startPromise.complete();
                } else {
                    LOG.error("Failed to start server at [{}]: {}", cli_port, handler.cause());
                    startPromise.fail(handler.cause());
                }
            });
        } else {
            LOG.error("PIVEAU_HUB_SEARCH_CLI_TYPE should be either \'http\', \'ssh\' or \'telnet\'");
            startPromise.fail("PIVEAU_HUB_SEARCH_CLI_TYPE should be either \'http\', \'ssh\' or \'telnet\'");
        }

        CommandBuilder triggerSitemapGeneration = CommandBuilder.command("triggerSitemapGeneration");
        triggerSitemapGeneration.processHandler(process -> sitemapsService.triggerSitemapGeneration(ar -> {
            if (ar.succeeded()) {
                process.write(ar.result().getString("result") + "\n");
                process.end();
            }
        }));

        CommandBuilder changeMaxAggSizeCommand = CommandBuilder.command("max_agg_size");
        changeMaxAggSizeCommand.processHandler(process -> {
            List<String> args = process.args();
            if (args.size() != 2) {
                process.write("max_agg_size: try \'max_agg_size index number (>0)\'\n");
                process.end();
            } else {
                try {
                    String index = args.get(0);
                    if (index.equals("dataset") || index.equals("catalogue") || index.equals("dataservice")) {
                        Integer max_agg_size = Integer.parseInt(args.get(1));
                        searchService.setMaxAggSize(index, max_agg_size, ar -> handleResponse(process, ar));
                    } else {
                        process.write("Choose dataset, catalogue or dataservice as index name!\n");
                    }
                } catch (NumberFormatException e) {
                    process.write("max_agg_size: try \'max_agg_size number (>0)\'\n");
                    process.end();
                }
            }
        });

        CommandBuilder mappingCommand = CommandBuilder.command("mapping");
        mappingCommand.processHandler(process -> {
            List<String> args = process.args();
            if (args.size() != 1) {
                process.write("mapping: try \'mapping index\'\n");
                process.end();
            } else {
                String index = args.get(0);
                if (index.equals("dataset") || index.equals("catalogue") || index.equals("dataservice")) {
                    searchService.mapping(index, ar -> handleResponse(process, ar));
                } else {
                    process.write("Choose dataset, catalogue or dataservice as index name!\n");
                    process.end();
                }
            }
        });

        CommandBuilder indexCommand = CommandBuilder.command("index");
        indexCommand.processHandler(process -> {
            List<String> args = process.args();
            if (args.size() != 2) {
                process.write("index: try \'index create/delete name\'\n");
                process.end();
            } else {
                String operation = args.get(0);
                String index = args.get(1);
                if (index.equals("dataset") || index.equals("catalogue") || index.equals("dataservice")) {
                    switch (operation) {
                        case "create":
                            searchService.indexCreate(index, ar -> handleResponse(process, ar));
                            break;
                        case "delete":
                            searchService.indexDelete(index, ar -> handleResponse(process, ar));
                            break;
                        default:
                            process.write("index: try \'index create/delete name\'\n");
                            process.end();
                            break;
                    }
                } else {
                    process.write("Choose dataset, catalogue or dataservice as index name!\n");
                    process.end();
                }
            }
        });

        CommandBuilder resetCommand = CommandBuilder.command("reset");
        resetCommand.processHandler(process -> {
            process.write("Are you sure you want to reset all indexes? [y/n]\n");
            process.interruptHandler(v -> process.end());
            process.stdinHandler(data -> {
                if (data.equals("y") || data.equals("Y")) {
                    List<Future> futureDeleteList = new ArrayList<>();

                    Promise<Void> deleteDatasetPromise = Promise.promise();
                    searchService.indexDelete("dataset", ar ->
                            handleResponseWithOutTerminating(deleteDatasetPromise, process, ar));
                    futureDeleteList.add(deleteDatasetPromise.future());

                    Promise<Void> deleteCataloguePromise = Promise.promise();
                    searchService.indexDelete("catalogue", ar ->
                            handleResponseWithOutTerminating(deleteCataloguePromise, process, ar));
                    futureDeleteList.add(deleteCataloguePromise.future());

                    Promise<Void> deleteDataServicePromise = Promise.promise();
                    searchService.indexDelete("dataservice", ar ->
                            handleResponseWithOutTerminating(deleteDataServicePromise, process, ar));
                    futureDeleteList.add(deleteDataServicePromise.future());

                    CompositeFuture.all(futureDeleteList).setHandler(deleteHandler -> {
                        if (deleteHandler.succeeded()) {
                            process.write("Successfully deleted all indexes\n");
                        } else {
                            process.write(deleteHandler.cause().toString());
                        }

                        List<Future> futureCreateList = new ArrayList<>();

                        Promise<Void> createDatasetPromise= Promise.promise();
                        searchService.indexCreate("dataset", ar ->
                                handleResponseWithOutTerminating(createDatasetPromise, process, ar));
                        futureCreateList.add(createDatasetPromise.future());

                        Promise<Void> createCataloguePromise = Promise.promise();
                        searchService.indexCreate("catalogue", ar ->
                                handleResponseWithOutTerminating(createCataloguePromise, process, ar));
                        futureCreateList.add(createCataloguePromise.future());

                        Promise<Void> createDataServicePromise = Promise.promise();
                        searchService.indexCreate("dataservice", ar ->
                                handleResponseWithOutTerminating(createDataServicePromise, process, ar));
                        futureCreateList.add(createDataServicePromise.future());

                        CompositeFuture.all(futureCreateList).setHandler(createHandler -> {
                            if (createHandler.succeeded()) {
                                process.write("Successfully created all indexes\n");
                                List<Future> futureMappingList = new ArrayList<>();

                                Promise<Void> mappingDatasetPromise = Promise.promise();
                                searchService.mapping("dataset", ar ->
                                        handleResponseWithOutTerminating(mappingDatasetPromise, process, ar));
                                futureMappingList.add(mappingDatasetPromise.future());

                                Promise<Void> mappingCataloguePromise = Promise.promise();
                                searchService.mapping("catalogue", ar ->
                                        handleResponseWithOutTerminating(mappingCataloguePromise, process, ar));
                                futureMappingList.add(mappingCataloguePromise.future());

                                Promise<Void> mappingDataServicePromise = Promise.promise();
                                searchService.mapping("dataservice", ar ->
                                        handleResponseWithOutTerminating(mappingDataServicePromise, process, ar));
                                futureMappingList.add(mappingDataServicePromise.future());

                                CompositeFuture.all(futureMappingList).setHandler(mappingHandler -> {
                                    if (mappingHandler.succeeded()) {
                                        process.write("Successfully put all mappings\n");
                                    } else {
                                        process.write(mappingHandler.cause().toString());
                                    }
                                    process.end();
                                });
                            } else {
                                process.write(createHandler.cause().toString());
                                process.end();
                            }
                        });
                    });
                } else if (data.equals("n") || data.equals("N")) {
                    process.end();
                }
            });
        });

        CommandBuilder changeMaxResultWindow = CommandBuilder.command("max_result_window");
        changeMaxResultWindow.processHandler(process -> {
            List<String> args = process.args();

            if (args.size() != 2) {
                process.write("max_result_window: try \'max_result_window index value\'\n");
                process.end();
            } else {
                try {
                    String index = args.get(0);
                    if (index.equals("dataset") || index.equals("catalogue") || index.equals("dataservice")) {
                        Integer max_result_window = Integer.parseInt(args.get(1));
                        searchService.setMaxResultWindow(index, max_result_window, ar -> handleResponse(process, ar));
                    } else {
                        process.write("Choose dataset, catalogue or dataservice as index name!\n");
                    }
                } catch (NumberFormatException e) {
                    process.write("Value should be an integer\n");
                    process.end();
                }
            }
        });

        CommandBuilder boost = CommandBuilder.command("boost");
        boost.processHandler(process -> {
            List<String> args = process.args();

            if (args.size() != 3) {
                process.write("boost: try \'boost type field value\'\n");
                process.end();
            } else {
                try {
                    String type = args.get(0);
                    String field = args.get(1);
                    Float value = Float.parseFloat(args.get(2));

                    searchService.boost(type, field, value, ar -> handleResponse(process, ar));
                } catch (NumberFormatException e) {
                    process.write("Value should be a float\n");
                    process.end();
                }
            }
        });

        CommandBuilder reindex_catalogues = CommandBuilder.command("reindex_catalogues");
        reindex_catalogues.processHandler(process -> {
            process.write("Are you sure you want to reindex all catalogues? [y/n]\n");
            process.interruptHandler(v -> process.end());
            process.stdinHandler(data -> {
                if (data.equals("y") || data.equals("Y")) {
                    JsonObject query = new JsonObject();
                    query.put("filter", "catalogue");
                    query.put("from", 0);
                    query.put("size", 1000);
                    query.put("elasticId", true);

                    List<Future> futureList = new ArrayList<>();
                    searchService.search(query.toString(), searchResult -> {
                        if (searchResult.succeeded()) {
                            JsonObject result = searchResult.result().getJsonObject("result");
                            JsonArray results = result.getJsonArray("results");
                            results.forEach(value -> {
                                Promise<Void> valuePromise = Promise.promise();
                                ((JsonObject) value).remove("count");
                                String id = ((JsonObject) value).remove("_id").toString();
                                final JsonObject valueJson = ((JsonObject) value);
                                /*cataloguesService.deleteCatalogue(id, deleteResult -> {
                                    if (deleteResult.succeeded()) {
                                        JsonObject messageDocumentReplace = new JsonObject();
                                        messageDocumentReplace.put("type", "catalogue");
                                        messageDocumentReplace.put("payload", valueJson.toString());
                                        messageDocumentReplace.put("replace", true);*/
                                cataloguesService.createOrUpdateCatalogue(id, valueJson,
                                        replaceResult -> {
                                            if (replaceResult.succeeded()) {
                                                process.write(valueJson.getString("id") + "\n");
                                                valuePromise.complete();
                                            } else {
                                                process.write(replaceResult.cause().getMessage() + "\n");
                                                valuePromise.complete();
                                            }
                                        });
                                    /*} else {
                                        process.write(deleteResult.cause().getMessage() + "\n");
                                        valueFuture.complete();
                                    }
                                });*/
                                futureList.add(valuePromise.future());
                            });

                            CompositeFuture.all(futureList).setHandler(ar -> process.end());
                        } else {
                            process.write(searchResult.cause().getMessage() + "\n");
                            process.end();
                        }
                    });
                } else if (data.equals("n") || data.equals("N")) {
                    process.end();
                }
            });
        });

        CommandBuilder loadMockdata = CommandBuilder.command("load-mockdata");
        loadMockdata.processHandler(process -> {
            List<String> args = process.args();
            if (args.size() != 2) {
                process.write("load-mockdata: try \'load-mockdata typeName numFiles\'\n");
                process.end();
            } else {
                try {
                    int num = Integer.parseInt(args.get(1));
                    List<Future> loadMockdataList = new ArrayList<>();
                    for (int i = 0; i < num; ++i) {
                        loadMockdataList.add(loadMockdata(process, args.get(0), i).future());
                    }
                    CompositeFuture.join(loadMockdataList).setHandler(handler -> {
                        if (handler.succeeded()) {
                            process.write("Successfully loaded mockdata\n");
                        } else {
                            process.write(handler.cause().toString());
                        }
                        process.end();
                    });
                } catch (NumberFormatException e) {
                    process.write("load-mockdata: numFiles has to be an integer\'\n");
                    process.end();
                }
            }
        });

        CommandRegistry registry = CommandRegistry.getShared(vertx);
        registry.registerCommand(indexCommand.build(vertx));
        registry.registerCommand(mappingCommand.build(vertx));
        registry.registerCommand(boost.build(vertx));
        registry.registerCommand(resetCommand.build(vertx));
        registry.registerCommand(changeMaxAggSizeCommand.build(vertx));
        registry.registerCommand(changeMaxResultWindow.build(vertx));
        registry.registerCommand(reindex_catalogues.build(vertx));
        registry.registerCommand(loadMockdata.build(vertx));
        registry.registerCommand(triggerSitemapGeneration.build(vertx));
    }

    private void handleResponse(CommandProcess process, AsyncResult<String> ar) {
        if (ar.succeeded()) {
            process.write(ar.result() + "\n");
        } else {
            process.write(ar.cause().getMessage() + "\n");
        }
        process.end();
    }

    private void handleResponseWithOutTerminating(Promise<Void> promise, CommandProcess process,
                                                  AsyncResult<String> ar) {
        if (ar.succeeded()) {
            process.write(ar.result() + "\n");
            promise.complete();
        } else {
            process.write(ar.cause().getMessage() + "\n");
            promise.fail(ar.cause().getMessage() + "\n");
        }
    }

    private Promise<Void> loadMockdata(CommandProcess process, String type, int current) {
        Promise<Void> promise = Promise.promise();

        vertx.fileSystem().readFile("test/mock/" + type + "/mock_" + current + ".json", mockHandler -> {
            if (mockHandler.succeeded()) {
                if (type.equals("dataset")) {
                    datasetsService.createOrUpdateDatasetBulk(new JsonArray(mockHandler.result().toString()), ar ->
                            handleSingleMock(process, type, current, promise, ar));
                } else if (type.equals("catalogue")) {

                    List<Future> futureList = new ArrayList<>();
                    for (Object obj : new JsonArray(mockHandler.result().toString())) {
                        JsonObject jsonObj = (JsonObject) obj;
                        String id = jsonObj.getString("id");

                        Promise<Void> catalogPromise = Promise.promise();
                        futureList.add(catalogPromise.future());
                        cataloguesService.createOrUpdateCatalogue(id, jsonObj, ar -> {
                            if (ar.succeeded()) {
                                catalogPromise.complete();
                            } else {
                                promise.fail(ar.cause().getMessage() + "\n");
                            }
                        });
                    }

                    CompositeFuture.all(futureList).setHandler(ar -> {
                        if (ar.succeeded()) {
                            process.write("Successfully loaded \'test/mock/" + type + "/mock_" + current + ".json\"\'\n");
                            promise.complete();
                        } else {
                            process.write(ar.cause().getMessage() + "\n");
                            promise.fail(ar.cause().getMessage() + "\n");
                        }
                    });
                }
            } else {
                process.write("Failed to open \'test/mock/" + type + "/mock_" + current + ".json\"\'\n");
                promise.fail("Failed to open \'test/mock/" + type + "/mock_" + current + ".json\"\'\n");
            }
        });

        return promise;
    }

    private void handleSingleMock(CommandProcess process, String type, int current, Promise<Void> promise,
                                  AsyncResult<JsonObject> ar) {
        if (ar.succeeded()) {
            process.write("Successfully loaded \'test/mock/" + type + "/mock_" + current + ".json\"\'\n");
            promise.complete();
        } else {
            process.write(ar.cause().getMessage() + "\n");
            promise.fail(ar.cause().getMessage() + "\n");
        }
    }
}
