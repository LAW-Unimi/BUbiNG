package it.unimi.di.law.warc.io.gzarc;

/*
 * Copyright (C) 2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertArrayEquals;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import com.google.common.io.ByteStreams;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.di.law.bubing.util.Util;
import it.unimi.di.law.warc.io.gzarc.GZIPArchive.ReadEntry;
import it.unimi.di.law.warc.io.gzarc.GZIPArchive.ReadEntry.LazyInflater;

public class GZIPArchiveWriterTest {

    /*
     * Populates an archive with the prescribed number of entries, returning the
     * written entries. If randomized the i-th entry is
     *
     * "MAGIC" + RandomStringUtils.randomAscii(1024 * (1 + i)) :
     *
     * if not randomized is
     *
     * "Hello, world " + i + "!\n"
     *
     */
    public static List<byte[]> writeArchive(final String path, final int n, final boolean randomized) throws IOException {
	FileOutputStream fos = new FileOutputStream(path);
	GZIPArchiveWriter gzaw = new GZIPArchiveWriter(fos);
	GZIPArchive.WriteEntry we;
	List<byte[]> contents = new ArrayList<>();
	for (int i = 0; i < n; i++) {
	    we = gzaw.getEntry("Test " + i, "Comment " + i, new Date());
	    final byte[] content = Util.toByteArray(
		    randomized ? "MAGIC" + RandomStringUtils.randomAscii(1024 * (1 + i)) : "Hello, world " + i + "!\n");
	    we.deflater.write(content);
	    contents.add(content);
	    we.deflater.close();
	}
	gzaw.close();
	return contents;
    }

    public static final String ARCHIVE_PATH = "/tmp/archive.gz";
    public static final int ARCHIVE_SIZE = 1024;

    @Test
    public void readWrite() throws IOException {
	List<byte[]> contents = writeArchive(ARCHIVE_PATH, ARCHIVE_SIZE, false);
	FileInputStream fis = new FileInputStream(ARCHIVE_PATH);
	GZIPArchiveReader gzar = new GZIPArchiveReader(fis);
	GZIPArchive.ReadEntry re;
	for (byte[] expected: contents) {
	    re = gzar.getEntry();
	    if (re == null) break;
	    LazyInflater lin = re.lazyInflater;
	    final byte[] actual = ByteStreams.toByteArray(lin.get());
	    assertArrayEquals(expected, actual);
	    lin.consume();
	}
	fis.close();
    }

    public static void main(String[] args) throws IOException, JSAPException {

	SimpleJSAP jsap = new SimpleJSAP(GZIPArchiveReader.class.getName(), "Writes some random records on disk.",
		new Parameter[] {
			new Switch("fully", 'f', "fully",
				"Whether to read fully the record (and do a minimal cosnsistency check)."),
			new UnflaggedOption("path", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY,
				"The path to read from."), });

	JSAPResult jsapResult = jsap.parse(args);
	if (jsap.messagePrinted())
	    System.exit(1);

	final boolean fully = jsapResult.getBoolean("fully");
	GZIPArchiveReader gzar = new GZIPArchiveReader(new FileInputStream(jsapResult.getString("path")));
	for (;;) {
	    ReadEntry e = gzar.getEntry();
	    if (e == null)
		break;
	    InputStream inflater = e.lazyInflater.get();
	    if (fully)
		ByteStreams.toByteArray(inflater);
	    e.lazyInflater.consume();
	    System.out.println(e);
	}
    }

}
