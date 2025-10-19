package com.pri1712.searchengine.wikiparser;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.XMLEvent;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
    public Parser(String XmlFilePath) {
        this.XmlFilePath = XmlFilePath;
    }

    public void parseData() throws Exception {
        if (XmlFilePath.isEmpty()) {
            throw new Exception("XML file path is empty");
        }
        try {
            fis = new FileInputStream(XmlFilePath);
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.WARNING, "File not found: " + XmlFilePath, e);
        }
        BufferedInputStream bis = new BufferedInputStream(fis);
        BZip2CompressorInputStream compressedInputStream = new BZip2CompressorInputStream(bis);
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLEventReader xmlEventReader = factory.createXMLEventReader(compressedInputStream);
        String title = "";
        StringBuilder text = new StringBuilder();
        String currentTag = "";
        String timestamp = "";
        String ID = "";
        while (xmlEventReader.hasNext()) {
            XMLEvent nextEvent = xmlEventReader.nextEvent();
            if (nextEvent.isStartDocument()) {
                LOGGER.log(Level.INFO, "Start document");
            } else if (nextEvent.isStartElement()) {
                currentTag = nextEvent.asStartElement().getName().getLocalPart();
            } else if (nextEvent.isCharacters()) {
                String data = nextEvent.asCharacters().getData();
                if (currentTag.equals("title")) {
                    title = data;
                } else if (currentTag.equals("text")) {
                    text.append(data);
                } else if (currentTag.equals("timestamp")) {
                    timestamp = data;
                } else if (currentTag.equals("id")) {
                    ID = data;
                }
            } else if (nextEvent.isEndElement()) {
                String endTag = nextEvent.asEndElement().getName().getLocalPart();
                if (endTag.equals("page")) {
                    NormalizeText normalizeText = new NormalizeText();
                    StringBuilder cleanText = normalizeText.cleanText(text);
                    WikiDocument wikiDocument = new WikiDocument(ID,title,text,timestamp);
                }
            }
        }


    }

    public String getXmlFilePath() {
        return XmlFilePath;
    }

    public void setXmlFilePath(String XmlFilepath) {
        this.XmlFilePath = XmlFilepath;
    }
}
