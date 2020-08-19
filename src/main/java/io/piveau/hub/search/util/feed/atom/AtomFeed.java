package io.piveau.hub.search.util.feed.atom;

import io.vertx.core.json.JsonObject;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.*;

@XmlRootElement(name="feed", namespace="http://www.w3.org/2005/Atom")
public class AtomFeed {

    public static class Author {
        @XmlElement
        private String name;
        @XmlElement
        private String uri;

        public Author() {
            this.name = "";
            this.uri = "";
        }

        public Author(String name, String uri) {
            this.name = name;
            this.uri = uri;
        }

        public String getName() {
            return name;
        }

        public String getUri() {
            return uri;
        }

        @Override
        public String toString() {
            return "Author{" +
                    "name='" + name + '\'' +
                    ", uri='" + uri + '\'' +
                    '}';
        }
    }

    public static class Summary {
        @XmlAttribute
        private String type;
        @XmlValue
        private String content;

        public Summary() {
            this.content = "";
            this.type = "";
        }

        public Summary(String content, String type) {
            this.content = content;
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public String getContent() {
            return content;
        }

        @Override
        public String toString() {
            return "Summary{" +
                    "type='" + type + '\'' +
                    ", content='" + content + '\'' +
                    '}';
        }
    }

    public static class Link {
        @XmlAttribute
        private String href;
        @XmlAttribute
        private String rel;
        @XmlAttribute
        private String type;

        public Link() {
            this.href = "";
            this.rel = "";
            this.type = "";
        }

        public Link(String href, String rel, String type) {
            this.href = href;
            this.rel = rel;
            this.type = type;
        }

        public String getHref() {
            return href;
        }

        public String getRel() {
            return rel;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return "Link{" +
                    "href='" + href + '\'' +
                    ", rel='" + rel + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    public static class Entry {
        @XmlAttribute(name = "lang", namespace="http://www.w3.org/XML/1998/namespace", required = true)
        private String xml_lang;
        @XmlElement
        private String id;
        @XmlElement
        private String title;
        @XmlElement
        private Summary summary;
        @XmlElement(name="link")
        private List<Link> links;
        @XmlElement
        private String published;
        @XmlElement
        private String updated;
        @XmlElement(name="link")
        private Link alternate;

        public Entry() {
            this.xml_lang = "";
            this.id = "";
            this.title = "";
            this.summary = new Summary();
            this.links = new ArrayList<>();
            this.published = "";
            this.updated = "";
            this.alternate = new Link();
        }

        public Entry(String xml_lang, String id, String title, String summary_content, String summary_type, String published,
                     String updated, String alternate_href) {
            this.xml_lang = xml_lang;
            this.id = id;
            this.title = title;
            this.summary = new Summary(summary_content, summary_type);
            this.links = new ArrayList<>();
            this.published = published;
            this.updated = updated;
            this.alternate = new Link(alternate_href, "alternate", null);
        }

        public void addLink(String href, String rel, String type) {
            links.add(new Link(href, rel, type));
        }

        public String getXml_lang() {
            return xml_lang;
        }

        public String getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public Summary getSummary() {
            return summary;
        }

        public List<Link> getLinks() {
            return links;
        }

        public String getPublished() {
            return published;
        }

        public String getUpdated() {
            return updated;
        }

        public Link getAlternate() {
            return alternate;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "xml_lang='" + xml_lang + '\'' +
                    ", id='" + id + '\'' +
                    ", title='" + title + '\'' +
                    ", summary=" + summary +
                    ", links=" + links +
                    ", published='" + published + '\'' +
                    ", updated='" + updated + '\'' +
                    ", alternate=" + alternate +
                    '}';
        }
    }

    @XmlAttribute(name = "lang", namespace="http://www.w3.org/XML/1998/namespace", required = true)
    private String xml_lang;
    @XmlElement
    private String title;
    @XmlElement(name="link")
    private Link alternate;
    @XmlElement(name="link")
    private Link self;
    @XmlElement
    private String id;
    @XmlElement
    private String updated;
    @XmlElement
    private Author author;
    @XmlElement
    private String subtitle;
    @XmlElement(name="link")
    private Link first;
    @XmlElement(name="link")
    private Link next;
    @XmlElement(name="link")
    private Link previous;
    @XmlElement(name="link")
    private Link last;
    @XmlElement(name="entry")
    private List<Entry> entries;

    public AtomFeed() {
        this.xml_lang = "";
        this.title = "";
        this.entries = new ArrayList<>();
        this.alternate = null;
        this.self = null;
        this.id = "";
        this.updated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date());
        this.author = null;
        this.subtitle = "";
        this.first = null;
        this.next = null;
        this.previous = null;
        this.last = null;
    }

    public AtomFeed(String xml_lang, String title, String id, String author_name, String author_uri,
                    String subtitle, JsonObject links) {
        this.xml_lang = xml_lang;
        this.title = title + " - Atom Feed";
        this.entries = new ArrayList<>();
        this.alternate = new Link(links.getString("alternate"), "alternate", null);
        this.self = new Link(links.getString("self"), "self", null);
        this.id = id;
        this.updated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date());
        this.author = new Author(author_name, author_uri);
        this.subtitle = subtitle;
        this.first = new Link(links.getString("first"), "first", null);
        if (links.getString("next") != null) {
            this.next = new Link(links.getString("next"), "next", null);
        } else {
            this.next = null;
        }
        if (links.getString("previous") != null) {
            this.previous = new Link(links.getString("previous"), "previous", null);
        } else {
            this.previous = null;
        }
        this.last = new Link(links.getString("last"), "last", null);
    }

    public Entry addEntry(String xml_lang, String id, String title, String summary_content, String summary_type, String published,
                          String updated, String alternate_href) {
        Entry entry = new Entry(xml_lang, id, title, summary_content, summary_type, published, updated, alternate_href);

        entries.add(entry);

        return entry;
    }

    public String getXml_lang() {
        return xml_lang;
    }

    public String getTitle() {
        return title;
    }

    public Link getAlternate() {
        return alternate;
    }

    public Link getSelf() {
        return self;
    }

    public String getId() {
        return id;
    }

    public String getUpdated() {
        return updated;
    }

    public Author getAuthor() {
        return author;
    }

    public String getSubtitle() {
        return subtitle;
    }

    public Link getFirst() {
        return first;
    }

    public Link getNext() {
        return next;
    }

    public Link getPrevious() {
        return previous;
    }

    public Link getLast() {
        return last;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    @Override
    public String toString() {
        return "AtomFeed{" +
                "xml_lang='" + xml_lang + '\'' +
                ", title='" + title + '\'' +
                ", alternate=" + alternate +
                ", self=" + self +
                ", id='" + id + '\'' +
                ", updated='" + updated + '\'' +
                ", author=" + author +
                ", subtitle='" + subtitle + '\'' +
                ", first=" + first +
                ", next=" + next +
                ", previous=" + previous +
                ", last=" + last +
                ", entries=" + entries +
                '}';
    }
}
