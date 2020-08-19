package io.piveau.hub.search.models;

final public class Constants {

    static public final String ENV_PIVEAU_HUB_SEARCH_SERVICE_PORT = "PIVEAU_HUB_SEARCH_SERVICE_PORT";
    static public final String ENV_PIVEAU_HUB_SEARCH_BOOST = "PIVEAU_HUB_SEARCH_BOOST";
    static public final String ENV_PIVEAU_HUB_SEARCH_API_KEY = "PIVEAU_HUB_SEARCH_API_KEY";
    static public final String ENV_PIVEAU_HUB_SEARCH_ES_CONFIG = "PIVEAU_HUB_SEARCH_ES_CONFIG";
    static public final String ENV_PIVEAU_HUB_SEARCH_CLI_CONFIG = "PIVEAU_HUB_SEARCH_CLI_CONFIG";

    static public final String ENV_PIVEAU_HUB_SEARCH_GAZETTEER_CONFIG = "PIVEAU_HUB_SEARCH_GAZETTEER_CONFIG";

    static public final String ENV_PIVEAU_HUB_SEARCH_SITEMAP_CONFIG = "PIVEAU_HUB_SEARCH_SITEMAP_CONFIG";

    static public final String ENV_PIVEAU_HUB_SEARCH_FEED_CONFIG = "PIVEAU_HUB_SEARCH_FEED_CONFIG";

    public enum Operator {
        AND,
        OR
    }
}
