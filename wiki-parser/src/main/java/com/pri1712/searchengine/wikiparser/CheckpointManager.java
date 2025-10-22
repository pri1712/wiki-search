package com.pri1712.searchengine.wikiparser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CheckpointManager {
    private Path outputDir;
    private final Logger LOGGER = Logger.getLogger(CheckpointManager.class.getName());
    public CheckpointManager(String outputDir) {
        this.outputDir = Paths.get(outputDir);
    }
    public Integer readCheckpointBatch()  {
        if (!Files.exists(outputDir)) {
            return -1;
        }
        //if it exists read it.
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(outputDir.toFile()));
            String line = bufferedReader.readLine();
            if (line == null || line.isBlank()) {
                return -1;
            }
            return Integer.parseInt(line.trim());
        } catch (NumberFormatException | FileNotFoundException e) {
            LOGGER.log(Level.WARNING, "Error while reading checkpoint batch count", e);
            return -1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void writeCheckpointBatch(int number) throws IOException {
        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputDir.toFile()))) {
            bufferedWriter.write(Integer.toString(number));
            bufferedWriter.newLine();
        } catch (IOException e){
            LOGGER.log(Level.WARNING,e.getMessage(),e);
            throw new IOException();
        }
    }
}
