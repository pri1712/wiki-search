package com.pri1712.searchengine.wikiutils;

import java.io.OutputStream;

public class CountingOutputStream extends OutputStream {
    long count = 0;
    @Override
    public void write(int b) {
        count++;
    }
    @Override
    public void write(byte[] b, int off, int len) {
        count += len;
    }
    public long getCount() { return count; }
}