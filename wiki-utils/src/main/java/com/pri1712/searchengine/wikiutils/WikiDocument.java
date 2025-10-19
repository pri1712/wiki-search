package com.pri1712.searchengine.wikiutils;

import java.util.logging.*;

public class WikiDocument {
    private String id;
    private String title;
    private String text;
    private String timestamp;
    private static final Logger LOGGER = Logger.getLogger(WikiDocument.class.getName());
    public WikiDocument(String id, String title, String text, String timestamp) {
        this.id = id;
        this.title = title;
        this.text = text;
        this.timestamp = timestamp;
        LOGGER.log(Level.INFO, "WikiDocument created with title: {0}", title);
        LOGGER.log(Level.INFO, "WikiDocument created with text: {0}", text);
        LOGGER.log(Level.INFO, "WikiDocument created with timestamp: {0}", timestamp);
        LOGGER.log(Level.INFO, "WikiDocument created with id: {0}", id);
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
