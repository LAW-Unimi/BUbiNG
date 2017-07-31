package it.unimi.di.law.bubing.store;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.warc.io.CompressedWarcWriter;
import it.unimi.di.law.warc.io.UncompressedWarcWriter;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpResponse;
import org.apache.http.message.HeaderGroup;

//RELEASE-STATUS: DIST

/** An unbuffered, directly-to-disk store, mainly for debugging purposes.
 *
 * @author Sebastiano Vigna
 */

public class UnbufferedFileStore implements Closeable, Store {
	public final int OUTPUT_STREAM_BUFFER_SIZE = 1024 * 1024;
	public final static String STORE_NAME = "store";
	private final FileOutputStream outputStream;
	private final WarcWriter writer;

	public UnbufferedFileStore(final RuntimeConfiguration rc) throws IOException {
		final File file = new File(rc.storeDir, STORE_NAME);
		if (rc.crawlIsNew) {
			if (file.exists() && file.length() != 0) throw new IOException("Store exists and it is not empty, but the crawl is new; it will not be overwritten: " + file);
			outputStream = new FileOutputStream(file);
		} else {
			if (!file.exists()) throw new IOException("Store does not exist, but the crawl is not new; it will not be created: " + file);
			outputStream = new FileOutputStream(file, true);
		}
		writer = STORE_NAME.endsWith(".gz") ? new CompressedWarcWriter(outputStream) : new UncompressedWarcWriter(outputStream);
	}

	@Override
	public synchronized void store(final URI uri, final HttpResponse response, boolean isDuplicate, final byte[] contentDigest, final String guessedCharset) throws IOException, InterruptedException {
		if (contentDigest == null) throw new NullPointerException("Content digest is null");
		final HttpResponseWarcRecord record = new HttpResponseWarcRecord(uri, response);
		HeaderGroup warcHeaders = record.getWarcHeaders();
		warcHeaders.updateHeader(new WarcHeader(WarcHeader.Name.WARC_PAYLOAD_DIGEST, "bubing:" + Hex.encodeHexString(contentDigest)));
		if (guessedCharset != null) warcHeaders.updateHeader(new WarcHeader(WarcHeader.Name.BUBING_GUESSED_CHARSET, guessedCharset));
		if (isDuplicate) warcHeaders.updateHeader(new WarcHeader(WarcHeader.Name.BUBING_IS_DUPLICATE, "true"));
		writer.write(record);
	}

	@Override
	public synchronized void close() throws IOException {
		writer.close();
	}
}
