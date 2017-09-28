package it.unimi.di.law.warc.io.gzarc;

/*
 * Copyright (C) 2013-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertArrayEquals;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import it.unimi.di.law.bubing.util.Util;
import it.unimi.di.law.warc.io.gzarc.GZIPArchive.ReadEntry.LazyInflater;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;

public class GZIPArchiveReaderTest {

    public static final String ARCHIVE_PATH = "/tmp/archive-random.gz";
    public static final int ARCHIVE_SIZE = 10;
    public static final byte[] EXPECTED_MAGIC = Util.toByteArray("MAGIC");

    @BeforeClass
    public static void setUp() throws IOException {
	GZIPArchiveWriterTest.writeArchive(ARCHIVE_PATH, ARCHIVE_SIZE, true);
    }

    @Test
    public void reverseGetEntry() throws IOException {
	final LongBigArrayBigList pos = GZIPIndexer.index(new FileInputStream(ARCHIVE_PATH));
	GZIPArchive.ReadEntry re;
	FastBufferedInputStream fis = new FastBufferedInputStream(new FileInputStream(ARCHIVE_PATH));
	GZIPArchiveReader gzar = new GZIPArchiveReader(fis);
	byte[] actualMagic = new byte[EXPECTED_MAGIC.length];
	for (int i = (int) pos.size64() - 1; i >= 0; i--) {
	    gzar.position(pos.getLong(i));
	    re = gzar.getEntry();
	    if (re == null) break;
	    LazyInflater lin = re.lazyInflater;
	    InputStream in = lin.get();
	    in.read(actualMagic);
	    assertArrayEquals(EXPECTED_MAGIC, actualMagic);
	    for (int j = 0; j < (i + 1) * 512; j++) in.read();
	    lin.consume();

	}
	fis.close();
    }

    @Test
    public void partialRead() throws IOException {
	FileInputStream fis = new FileInputStream(ARCHIVE_PATH);
	GZIPArchiveReader gzar = new GZIPArchiveReader(fis);
	byte[] actualMagic = new byte[EXPECTED_MAGIC.length];
	for (int i = 0; i < ARCHIVE_SIZE + 1; i++) {
	    GZIPArchive.ReadEntry re = gzar.getEntry();
	    if (re == null) break;
	    LazyInflater lin = re.lazyInflater;
	    InputStream in = lin.get();
	    in.read(actualMagic);
	    assertArrayEquals(EXPECTED_MAGIC, actualMagic);
	    for (int j = 0; j < (i + 1) * 512; j++) in.read();
	    lin.consume();
	}
	fis.close();
    }

    @Test
    public void fullRead() throws IOException {
	FileInputStream fis = new FileInputStream(ARCHIVE_PATH);
	GZIPArchiveReader gzar = new GZIPArchiveReader(fis);
	byte[] actualMagic = new byte[EXPECTED_MAGIC.length];
	fis = new FileInputStream(ARCHIVE_PATH);
	gzar = new GZIPArchiveReader(fis);
	for (int i = 0; i < ARCHIVE_SIZE + 1; i++) {
	    GZIPArchive.ReadEntry re = gzar.getEntry();
	    if (re == null) break;
	    LazyInflater lin = re.lazyInflater;
	    InputStream in = lin.get();
	    in.read(actualMagic);
	    assertArrayEquals(EXPECTED_MAGIC, actualMagic);
	    while (in.read() != -1);
	    lin.consume();
	}
	fis.close();
    }

    @Test
    public void skip() throws IOException {
	FileInputStream fis = new FileInputStream(ARCHIVE_PATH);
	GZIPArchiveReader gzar = new GZIPArchiveReader(fis);
	byte[] actualMagic = new byte[EXPECTED_MAGIC.length];
	fis = new FileInputStream(ARCHIVE_PATH);
	gzar = new GZIPArchiveReader(fis);
	for (int i = 0; i < ARCHIVE_SIZE + 1; i++) {
	    GZIPArchive.ReadEntry re = gzar.getEntry();
	    if (re == null) break;
	    LazyInflater lin = re.lazyInflater;
	    InputStream in = lin.get();
	    in.read(actualMagic);
	    assertArrayEquals(EXPECTED_MAGIC, actualMagic);
	    in.skip(Long.MAX_VALUE);
	    lin.consume();
	}
	fis.close();
    }

    @Ignore("This needs work")
    @Test
    public void cached() throws IOException {
	FileInputStream fis = new FileInputStream(ARCHIVE_PATH);
	GZIPArchiveReader gzar = new GZIPArchiveReader(fis);
	byte[] actualMagic = new byte[EXPECTED_MAGIC.length];
	fis = new FileInputStream(ARCHIVE_PATH);
	gzar = new GZIPArchiveReader(fis);
	for (int i = 0; i < ARCHIVE_SIZE + 1; i++) {
	    GZIPArchive.ReadEntry re = gzar.getEntry(true);
	    if (re == null) break;
	    LazyInflater lin = re.lazyInflater;
	    for (int repeat = 0; repeat < 3; repeat++) {
		InputStream in = lin.get(); // can get as many times you want
		in.read(actualMagic);
		assertArrayEquals(EXPECTED_MAGIC, actualMagic);
		in.skip(Long.MAX_VALUE);
	    }
	    lin.consume(); // must consume just once!
	}
	fis.close();
    }

}
