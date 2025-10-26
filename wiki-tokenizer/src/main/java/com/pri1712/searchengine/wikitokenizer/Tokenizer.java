package com.pri1712.searchengine.wikitokenizer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pri1712.searchengine.wikiutils.WikiDocument;

public class Tokenizer {
    private static final Logger LOGGER = Logger.getLogger(Tokenizer.class.getName());
    ObjectMapper mapper = new ObjectMapper();
    TokenNormalizer tokenNormalizer = new TokenNormalizer();

    public Tokenizer() throws RuntimeException {}

    public void tokenizeData(String parsedFilePath) {
        Path parsedPath = Paths.get(parsedFilePath);
        try (Stream<Path> fileStream = Files.list(parsedPath).filter(f -> f.toString().endsWith(".json.gz"))) {
            fileStream.forEach(file -> {
                try {
                   processFile(file);
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE,"IO exception while reading compressed json files",e);
                }
            });
        } catch (IOException e ) {
            throw new RuntimeException(e);
        }

    }

    private void processFile(Path file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file.toString());
        GZIPInputStream gis = new GZIPInputStream(fis);
        BufferedReader buffRead = new BufferedReader(new InputStreamReader(gis))) {
            List<WikiDocument> jsonDocuments = mapper.readValue(buffRead, new TypeReference<List<WikiDocument>>() {});
            for (WikiDocument wikiDocument : jsonDocuments) {
//                System.out.printf("Title of the document is: %s %n", wikiDocument.getTitle());
                //normalize then tokenize.
                WikiDocument normalizedDocument =  tokenNormalizer.normalizeData(wikiDocument);
                TokenizedData tokenizedText = tokenNormalizer.tokenizeText(normalizedDocument); //tokenizedText now has the tokenized title and text for the document.

            }

        }

    }

}