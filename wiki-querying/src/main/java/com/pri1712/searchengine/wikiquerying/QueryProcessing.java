package com.pri1712.searchengine.wikiquerying;
import com.pri1712.searchengine.wikitokenizer.TokenNormalizer;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class QueryProcessing {
    private TokenNormalizer tokenNormalizer;
    private String indexedFilePath;
    public QueryProcessing(){
       this.tokenNormalizer=new TokenNormalizer();
    }

    public List<String> processQuery(String query){
        //normalises and tokenise the query and returns a list of tokenised query
        String normalizedQuery=tokenNormalizer.normalizeHelper(query);
        return tokenNormalizer.tokenizerHelper(normalizedQuery, false);
    }

    public List<String>search(List<String>processedQuery,String indexedFilePath){

        Path indexedFile= Paths.get(indexedFilePath);
        try(Stream<Path>fileStream= Files.list(indexedFile)
                .filter(f->f.toString().endsWith(".json.gz"))
                .filter(f->f.getFileName().toString().startsWith("merged"))){



        }catch(IOException e){
            e.printStackTrace();
        }
    }

}
