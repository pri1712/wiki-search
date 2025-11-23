package com.pri1712.searchengine.utils;

import java.util.logging.*;

public class WikiDocument {
    private static final Logger LOGGER = Logger.getLogger(WikiDocument.class.getName());

    private String id;
    private String title;
    private String text;
    private String timestamp;

    public WikiDocument() {}
    public WikiDocument(String id, String title, String text, String timestamp) {
        this.id = id;
        this.title = title;
        this.text = text;
        this.timestamp = timestamp;
//        LOGGER.info(String.format(
//                "Parsed Page -> ID: %s | Title: %s | Text length: %d | Timestamp: %s",
//                id,
//                title,
//                text.length(),
//                timestamp
//        ));
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
