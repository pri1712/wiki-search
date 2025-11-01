package com.pri1712.searchengine.wikiutils;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;

public class BatchFileWriter {
    private static final Logger LOGGER = Logger.getLogger(BatchFileWriter.class.getName());

    private final String outputDir;
    ObjectMapper mapper = new ObjectMapper()
            .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    public  BatchFileWriter(String outputDir) throws IllegalArgumentException {
        if (outputDir==null || outputDir.trim().isEmpty()) {
            throw new IllegalArgumentException("outputDir is null or empty");
        }
        if(outputDir.endsWith("/")) {
            this.outputDir=outputDir;
        } else {
            this.outputDir=outputDir+"/";
        }
        new File(this.outputDir).mkdirs();
    }

    public void writeBatch(List<?> batch, int batchCount) {
        String outputFile = String.format("%sbatch_%05d.json.gz", outputDir, batchCount);
        try {
            FileOutputStream fos = new FileOutputStream(outputFile);
            GZIPOutputStream gos = new GZIPOutputStream(fos);
            mapper.writeValue(gos, batch);
            LOGGER.info(String.format("successfully wrote %d batches to file %s", batchCount+1, outputFile));
        } catch (IOException e) {
            throw new RuntimeException("Failed to write batch to file "+outputFile,e);
        }
        Runtime rt = Runtime.getRuntime();
        long used = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        LOGGER.info(String.format("Heap used: %d MB", used));
    }

    public void writeIndex(Map<String, Map<Integer,Integer>> invertedIndex,int batchCount) {
        String outputFile = String.format("%sindex_%05d.json.gz", outputDir, batchCount);
        try {
            FileOutputStream fos = new FileOutputStream(outputFile);
            GZIPOutputStream gos = new GZIPOutputStream(fos);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(gos));
            for (var entry : invertedIndex.entrySet()) {
                mapper.writeValue(bw, Map.of(entry.getKey(), entry.getValue()));
                bw.newLine();
            }
            LOGGER.info(String.format("Wrote index batch %05d (%d terms) to %s",
                    batchCount, invertedIndex.size(), outputFile));

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error while writing index",e);
            LOGGER.info(String.format("Failed to write index %s", outputFile));
        }
        Runtime rt = Runtime.getRuntime();
        long used = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        LOGGER.info(String.format("Heap used: %d MB", used));
    }
}
