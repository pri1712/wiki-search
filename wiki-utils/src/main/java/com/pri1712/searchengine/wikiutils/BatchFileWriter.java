package com.pri1712.searchengine.wikiutils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BatchFileWriter {
    private static final Logger LOGGER = Logger.getLogger(BatchFileWriter.class.getName());

    private String outputDir;

    ObjectMapper mapper = new ObjectMapper();

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

    public void WriteBatch(ArrayList<WikiDocument> batch, int batchCount) {
        String outputFile = String.format("%sbatch_%05d.json.gz", outputDir, batchCount);
        try {
            FileOutputStream fos = new FileOutputStream(outputFile);
            GZIPOutputStream gos = new GZIPOutputStream(fos);
            mapper.writeValue(gos, batch);
            LOGGER.info(String.format("successfully wrote %d batches to file %s", batchCount, outputFile));
        } catch (IOException e) {
            throw new RuntimeException("Failed to write batch to file "+outputFile,e);
        }

    }
}
