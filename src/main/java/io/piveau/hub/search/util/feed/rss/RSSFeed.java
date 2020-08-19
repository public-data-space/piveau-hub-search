package io.piveau.hub.search.util.feed.rss;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="rss")
public class RSSFeed {

    public static class Enclosure {
        @XmlAttribute
        private String url;
        @XmlAttribute
        private String type;

        public Enclosure() {
            this.url = "";
            this.type = "";
        }

        public Enclosure(String url, String type) {
            this.url = url;
            this.type = type;
        }

        public String getUrl() {
            return url;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return "Enclosure{" +
                    "url='" + url + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    public static class Item {
        @XmlElement
        private String guid;
        @XmlElement
        private String title;
        @XmlElement
        private String link;
        @XmlElement
        private String description;
        @XmlElement(name="enclosure")
        private List<RSSFeed.Enclosure> enclosures;
        @XmlElement
        private String pubDate;

        public Item() {
            this.guid = "";
            this.title = "";
            this.link = "";
            this.description = "";
            this.enclosures = new ArrayList<>();
            this.pubDate = "";
        }

        public Item(String guid, String title, String link, String description, String pubDate) {
            this.guid = guid;
            this.title = title;
            this.link = link;
            this.description = description;
            this.enclosures = new ArrayList<>();
            this.pubDate = pubDate;
        }

        public void addEnclosure(String url, String type) {
            enclosures.add(new RSSFeed.Enclosure(url, type));
        }

        public String getGuid() {
            return guid;
        }

        public String getTitle() {
            return title;
        }

        public String getLink() {
            return link;
        }

        public String getDescription() {
            return description;
        }

        public List<Enclosure> getEnclosures() {
            return enclosures;
        }

        public String getPubDate() {
            return pubDate;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "guid='" + guid + '\'' +
                    ", title='" + title + '\'' +
                    ", link='" + link + '\'' +
                    ", description='" + description + '\'' +
                    ", enclosures=" + enclosures +
                    ", pubDate='" + pubDate + '\'' +
                    '}';
        }
    }

    public static class Channel {
        @XmlElement
        private String title;
        @XmlElement
        private String link;
        @XmlElement
        private String description;
        @XmlElement
        private String language;
        @XmlElement
        private String copyright;
        @XmlElement
        private String pubDate;
        @XmlElement(name="item")
        private List<RSSFeed.Item> items;

        public Channel() {
            this.title = "";
            this.link = "";
            this.description = "";
            this.language = "";
            this.copyright = "";
            this.pubDate = "";
            this.items = new ArrayList<>();
        }

        public Channel(String title, String link, String description, String language, String copyright) {
            this.title = title + " - RSS Feed";
            this.link = link;
            this.description = description;
            this.language = language;
            this.copyright = copyright;
            this.pubDate = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z").format(new Date());
            this.items = new ArrayList<>();
        }

        public List<Item> getItems() {
            return items;
        }

        public String getTitle() {
            return title;
        }

        public String getLink() {
            return link;
        }

        public String getDescription() {
            return description;
        }

        public String getLanguage() {
            return language;
        }

        public String getCopyright() {
            return copyright;
        }

        public String getPubDate() {
            return pubDate;
        }

        @Override
        public String toString() {
            return "Channel{" +
                    "title='" + title + '\'' +
                    ", link='" + link + '\'' +
                    ", description='" + description + '\'' +
                    ", language='" + language + '\'' +
                    ", copyright='" + copyright + '\'' +
                    ", pubDate='" + pubDate + '\'' +
                    ", items=" + items +
                    '}';
        }
    }

    @XmlAttribute
    private final String version = "2.0";
    @XmlElement
    private Channel channel;

    public RSSFeed() {
        this.channel = null;
    }

    public RSSFeed(String title, String link, String description, String language, String copyright) {
        this.channel = new Channel(title, link, description, language, copyright);
    }

    public RSSFeed.Item addItem(String guid, String title, String link, String description, String pubDate) {
        RSSFeed.Item item = new RSSFeed.Item(guid, title, link, description, pubDate);

        channel.getItems().add(item);

        return item;
    }

    public String getVersion() {
        return version;
    }

    public Channel getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return "RSSFeed{" +
                "version='" + version + '\'' +
                ", channel=" + channel +
                '}';
    }
}
