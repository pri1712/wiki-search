package com.pri1712.searchengine.wikiutils;

import java.util.logging.*;
public class NormalizeText {

    private static Logger LOGGER = Logger.getLogger(NormalizeText.class.getName());

    public StringBuilder cleanText(StringBuilder text) {
        String lowercaseText = text.toString().toLowerCase();
//        LOGGER.log(Level.INFO,"Clean text: {0}", lowercaseText);
        if (lowercaseText.matches("(?i)^#redirect\\s*\\[\\[.*\\]\\]")) {
            LOGGER.log(Level.INFO,"has a redirect text");
            return new StringBuilder();
        }
        String cleanText = lowercaseText.replaceAll("[^a-z0-9\\s]"," ");
        cleanText = cleanText.replaceAll("\\s+", " ").trim();
        StringBuilder cleanSb =new StringBuilder(cleanText);
        return cleanSb;
    }

}
