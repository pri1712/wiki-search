package com.pri1712.searchengine.tokenizer;

import com.pri1712.searchengine.utils.WikiDocument;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tartarus.snowball.ext.PorterStemmer;

public class TokenNormalizer {
    private static final Logger LOGGER= Logger.getLogger(TokenNormalizer.class.getName());
    private static final Set<String> ENGLISH_STOP_WORDS = Set.of("a", "an", "the", "of", "at",
            "on", "upon", "in", "to", "from", "out", "as", "so", "such", "or", "and", "those", "this", "these", "that",
            "for", ",", "is", "was", "am", "are", "'s", "been", "were");
    PorterStemmer stemmer = new PorterStemmer();
    TokenizedData tokenizedData;

    public TokenNormalizer() {}

    public WikiDocument normalizeData(WikiDocument wikiDocument) {
        //normalize diacritics, remove unnecessary characters and spaces.
        WikiDocument normalizedDocument = new WikiDocument();
        String originalText = wikiDocument.getText();
        String originalTitle = wikiDocument.getTitle();
        String normalizedTitle = Normalizer.normalize(originalTitle, Normalizer.Form.NFD);
        normalizedTitle = normalizedTitle.replaceAll("\\p{M}","");
        normalizedTitle = normalizedTitle.replaceAll("[^a-z0-9]"," ");
        normalizedTitle = normalizedTitle.replaceAll("\\s+"," ");
        normalizedDocument.setTitle(normalizedTitle);

        String normalizedText = Normalizer.normalize(originalText, Normalizer.Form.NFD);
        normalizedText = normalizedText.replaceAll("\\p{M}","");
        normalizedText = normalizedText.replaceAll("[^a-z0-9]"," ");
        normalizedText = normalizedText.replaceAll("\\s+"," ");
        normalizedDocument.setText(normalizedText);
        normalizedDocument.setId(wikiDocument.getId());
        normalizedDocument.setTimestamp(wikiDocument.getTimestamp());
        return normalizedDocument;
    }

    public TokenizedData tokenizeText(WikiDocument wikiDocument) {
        //tokenizes, implements stemming and stopword removal.
        String originalText = wikiDocument.getText();
        String originalTitle = wikiDocument.getTitle();
        Pattern pattern = Pattern.compile("\\p{L}+");
        Matcher textMatcher = pattern.matcher(originalText);
        Matcher titleMatcher = pattern.matcher(originalTitle);
        List<List<String>> tokens = new ArrayList<>();
        List<String> textToken = new ArrayList<>();
        List<String> titleToken = new ArrayList<>();
        while (textMatcher.find()) {
            String token = textMatcher.group();
//            LOGGER.info("token: "+token+" "+originalTitle);
            if (token.length() <= 1 || ENGLISH_STOP_WORDS.contains(token)) {
                continue;
            } else {
                stemmer.setCurrent(token);
                stemmer.stem();
                token = stemmer.getCurrent();
                LOGGER.fine("adding token: " + token + " to text tokens list");
                textToken.add(token);
            }
        }

        while (titleMatcher.find()) {
            String token = titleMatcher.group();
//            LOGGER.info("token: "+token+" "+originalTitle);
            if (token.length() <= 1 ) {
                continue;
            } else {
                stemmer.setCurrent(token);
                stemmer.stem();
                token = stemmer.getCurrent();
                LOGGER.fine("adding token: " + token + " to title tokens list");
                titleToken.add(token);
            }
        }
        tokenizedData = new TokenizedData(textToken, titleToken,wikiDocument.getId());
        return tokenizedData;
    }
}
