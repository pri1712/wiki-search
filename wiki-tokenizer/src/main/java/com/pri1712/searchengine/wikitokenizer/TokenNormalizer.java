package com.pri1712.searchengine.wikitokenizer;

import com.pri1712.searchengine.wikiutils.WikiDocument;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TokenNormalizer {
    private static final Set<String> stopWords = Set.of("")

    public TokenNormalizer() {}

    public WikiDocument normalizeData(WikiDocument wikiDocument) {
        //normalize diacritics, remove unnecessary characters and spaces.
        WikiDocument normalizedDocument = new WikiDocument();
        String originalText = wikiDocument.getText();
        String originalTitle = wikiDocument.getTitle();
        String normalizedText = Normalizer.normalize(originalText, Normalizer.Form.NFD);
        String normalizedTitle = Normalizer.normalize(normalizedText, Normalizer.Form.NFD);
        normalizedTitle = normalizedTitle.replaceAll("\\p{M}","");
        normalizedTitle = normalizedTitle.replaceAll("[^a-z0-9]"," ");
        normalizedTitle = normalizedTitle.replaceAll("\\s+"," ");
        normalizedDocument.setTitle(normalizedTitle);
        normalizedText = normalizedText.replaceAll("\\p{M}","");
        normalizedText = normalizedText.replaceAll("[^a-z0-9]"," ");
        normalizedText = normalizedText.replaceAll("\\s+"," ");
        normalizedDocument.setText(normalizedText);
        return normalizedDocument;
    }

    public WikiDocument tokenizeText(WikiDocument wikiDocument) {
        //tokenizes, implements stemming and stopword removal.
        WikiDocument normalizedDocument = new WikiDocument();
        String originalText = wikiDocument.getText();
        String originalTitle = wikiDocument.getTitle();
        Pattern pattern = Pattern.compile("\\p{L}");
        Matcher textMatcher = pattern.matcher(originalText);
        Matcher titleMatcher = pattern.matcher(originalTitle);


    }
}
