package com.pri1712.searchengine.indexreader;

import java.util.List;

public class IndexData {
    private List<Integer> docIds;
    private List<Integer> freqs;
    public IndexData(List<Integer> docIds, List<Integer> freqs) {
        this.docIds = docIds;
        this.freqs = freqs;
    }

    public List<Integer> getDocIds() {
        return docIds;
    }

    public void setDocIds(List<Integer> docIds) {
        this.docIds = docIds;
    }

    public List<Integer> getFreqs() {
        return freqs;
    }

    public void setFreqs(List<Integer> freqs) {
        this.freqs = freqs;
    }
}
