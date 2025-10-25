package com.pri1712.searchengine.wikitokenizer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class Tokenizer {
    private static final Logger LOGGER = Logger.getLogger(Tokenizer.class.getName());

    public Tokenizer() throws RuntimeException {}

    public void TokenizeData(String parsedFilePath) {
        Path parsedPath = Paths.get(parsedFilePath);
        try (Stream<Path> fileStream = Files.list(parsedPath)) {
            fileStream.forEach(file -> {
//                LOGGER.info("Parsing Wikipedia XML dump file: " + file.getFileName().toString());

            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}