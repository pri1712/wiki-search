package com.pri1712.searchengine.wikiparser;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import java.io.*;
import java.util.ArrayList;
import java.util.logging.*;

import com.pri1712.searchengine.wikiutils.NormalizeText;
import com.pri1712.searchengine.wikiutils.WikiDocument;
import com.pri1712.searchengine.wikiparser.BatchFileWriter;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;


public class Parser {
    private static final Logger LOGGER = Logger.getLogger(Parser.class.getName());

    private static final int MAX_BATCH_SIZE = 1;

    private String XmlFilePath;
    private FileInputStream fis;
    private int docCounter = 0;
    private int batchCounter = 0;
    private int previousBatchCounter = 0;
    private final String batchCheckpointFile = "parserCheckpoint.txt";

    private ArrayList<WikiDocument> writeBuffer = new ArrayList<>();

    private BatchFileWriter batchFileWriter = new BatchFileWriter("parsed-data/");
    private CheckpointManager checkpointManager = new CheckpointManager(batchCheckpointFile);

    public Parser(String XmlFilePath) {
        this.XmlFilePath = XmlFilePath;
        this.previousBatchCounter = checkpointManager.readCheckpointBatch();
    }


    public void parseData() throws FileNotFoundException {
        if (XmlFilePath == null || XmlFilePath.isEmpty()) {
            throw new FileNotFoundException();
        }
        try {
            fis = new FileInputStream(XmlFilePath);
            BufferedInputStream bis = new BufferedInputStream(fis);
            XMLInputFactory factory = XMLInputFactory.newInstance();
            BZip2CompressorInputStream compressedStream = new BZip2CompressorInputStream(bis);
            XMLEventReader eventReader = factory.createXMLEventReader(compressedStream);
            StringBuilder textBuilder = new StringBuilder();
            StringBuilder titleBuilder = new StringBuilder();
            String timestamp = "";
            String ID = "";

            boolean inTitle = false;
            boolean inText = false;
            boolean inTimestamp = false;
            boolean inID = false;
            boolean firstID = true;

            while (eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();
                switch (event.getEventType()) {
                    case XMLStreamConstants.START_ELEMENT:
                        String currentTag = event.asStartElement().getName().getLocalPart();
                        switch (currentTag) {
                            case "title":
                                inTitle = true;
                                break;
                            case "text":
                                inText = true;
                                break;
                            case "timestamp":
                                inTimestamp = true;
                                break;
                            case "id":
                                if (firstID) {
                                    inID = true;
                                }
                        }
                        break;
                    case XMLStreamConstants.CHARACTERS:
                        String data = event.asCharacters().getData();
                        if (inTitle) {
                            titleBuilder.append(data);
                            break;
                        } else if  (inText) {
                            textBuilder.append(data);
                            break;
                        } else if (inTimestamp) {
                            timestamp += data;
                            break;
                        } else if (inID) {
                            ID +=data;
                            break;
                        }
                        break;
                    case XMLStreamConstants.END_ELEMENT:
                        String currentEndTag = event.asEndElement().getName().getLocalPart();
                        switch (currentEndTag) {
                            case "title":
                                inTitle = false;
                                break;
                            case "text":
                                inText = false;
                                break;
                            case "timestamp":
                                inTimestamp = false;
                                break;
                            case "id":
                                if (inID) {
                                    inID = false;
                                    firstID = false;
                                    //only read one ID per page.
                                }
                                break;
                            case "page":
                                //reached the end of the page.
                                NormalizeText normalizeText = new NormalizeText();
                                StringBuilder cleanText = normalizeText.cleanText(textBuilder);
                                StringBuilder cleanTitle = normalizeText.cleanText(titleBuilder);
                                //normalize and clean up title and text.
//                              LOGGER.log(Level.INFO,"Clean text length: {0}", cleanText.length());
                                if (cleanText.isEmpty()) {
//                                    LOGGER.log(Level.WARNING,"Clean text is empty for title: {0)", cleanTitle.toString());
                                    break;
                                }
                                WikiDocument wikiDocument = new WikiDocument(ID.trim(),cleanTitle.toString().trim(),cleanText.toString(),timestamp.trim());
                                writeBuffer.add(wikiDocument);
                                if (writeBuffer.size() >=  MAX_BATCH_SIZE) {
                                    //10k docs in the write buffer, need to write it to a file on disk.
                                    //check if this is blocking or non blocking.
                                    ArrayList<WikiDocument> newWriteBuffer = new ArrayList<>(writeBuffer);
                                    writeBuffer.clear();
                                    LOGGER.info(String.format("Previous batch counter was %d and new batch counter is %d",previousBatchCounter,batchCounter));
                                    if (previousBatchCounter == -1 || batchCounter > previousBatchCounter) {
                                        batchFileWriter.WriteBatch(newWriteBuffer,batchCounter);
                                    }
                                    checkpointManager.writeCheckpointBatch(batchCounter); //checkpoint it.
                                    batchCounter++;
                                }
                                //clean up modified data.
                                titleBuilder.setLength(0);
                                textBuilder.setLength(0);
                                timestamp = "";
                                ID = "";
                                firstID = true;
                                docCounter++;
                                if (docCounter > 5) {
                                    return;
                                }
                        }
                }
            }
        } catch (IOException | RuntimeException | XMLStreamException e ) {
            throw new RuntimeException(e);
        }
    }

    public String getXmlFilePath() {
        return XmlFilePath;
    }

    public void setXmlFilePath(String XmlFilepath) {
        this.XmlFilePath = XmlFilepath;
    }
}
