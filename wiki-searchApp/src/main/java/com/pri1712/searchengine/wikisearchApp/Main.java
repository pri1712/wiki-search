package com.pri1712.searchengine.wikisearchApp;

import com.pri1712.searchengine.wikiparser.Parser;
import com.pri1712.searchengine.wikiutils.WikiDocument;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter path to Wikipedia XML dump file: ");
        String filePath = scanner.nextLine().trim();
        scanner.close();

        try {
            Parser parser = new Parser(filePath);
            parser.parseData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}