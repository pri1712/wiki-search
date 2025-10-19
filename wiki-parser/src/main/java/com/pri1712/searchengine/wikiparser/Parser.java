package com.pri1712.searchengine.wikiparser;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.*;

import com.pri1712.searchengine.wikiutils.NormalizeText;
import com.pri1712.searchengine.wikiutils.WikiDocument;
import com.pri1712.searchengine.wikiutils.NormalizeText.*;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import com.pri1712.searchengine.wikiutils.WikiDocument;

public class Parser {
    private String XmlFilePath;
    private static final Logger LOGGER = Logger.getLogger(Parser.class.getName());
    private FileInputStream fis;
    private int docCounter = 0;

    public Parser(String XmlFilePath) {
        this.XmlFilePath = XmlFilePath;
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
                                WikiDocument wikiDocument = new WikiDocument(ID.trim(),titleBuilder.toString().trim(),textBuilder.toString(),timestamp.trim());

                                LOGGER.info("Parsed: " + wikiDocument.getTitle());
                                //clean up modified data.
                                titleBuilder.setLength(0);
                                textBuilder.setLength(0);
                                timestamp = "";
                                ID = "";
                                firstID = true;
                                docCounter++;
                                if (docCounter == 10) {
                                    return;
                                }
                        }
                }
            }
        } catch (IOException | XMLStreamException e) {
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
