package com.pri1712.searchengine.wikiutils;

public class WikiDocument {
    private String id;
    private String title;
    private StringBuilder text;
    private String timestamp;

    public WikiDocument(String id, String title, StringBuilder text, String timestamp) {
        this.id = id;
        this.title = title;
        this.text = text;
        this.timestamp = timestamp;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public StringBuilder getText() {
        return text;
    }

    public void setText(StringBuilder text) {
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
