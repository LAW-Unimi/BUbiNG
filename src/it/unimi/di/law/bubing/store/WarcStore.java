package it.unimi.di.law.bubing.store;

/*
 * Copyright (C) 2012-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.warc.io.ParallelBufferedWarcWriter;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpResponse;
import org.apache.http.message.HeaderGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** A {@link Store} implementation using the {@link it.unimi.di.law.warc} package. */

public class WarcStore implements Closeable, Store {
	private final static Logger LOGGER = LoggerFactory.getLogger(WarcStore.class);

	public final int OUTPUT_STREAM_BUFFER_SIZE = 1024 * 1024;
	public final static String STORE_NAME = "store.warc.gz";
	public final static String DIGESTS_NAME = "digests.bloom";
	public final static int NUM_GZ_WARC_RECORDS = 16;

	private final FastBufferedOutputStream outputStream;

	private final ParallelBufferedWarcWriter writer;

	public WarcStore(final RuntimeConfiguration rc) throws IOException {
		final File file = new File(rc.storeDir, STORE_NAME);

		if (rc.crawlIsNew) {
			if (file.exists() && file.length() != 0) throw new IOException("Store exists and it is not empty, but the crawl is new; it will not be overwritten: " + file);
			outputStream = new FastBufferedOutputStream(new FileOutputStream(file), OUTPUT_STREAM_BUFFER_SIZE);
		} else {
			if (!file.exists()) throw new IOException("Store does not exist, but the crawl is not new; it will not be created: " + file);
			outputStream = new FastBufferedOutputStream(new FileOutputStream(file, true), OUTPUT_STREAM_BUFFER_SIZE);
		}
		writer = new ParallelBufferedWarcWriter(outputStream, true);
	}

	@Override
	public void store(final URI uri, final HttpResponse response, final boolean isDuplicate, final byte[] contentDigest, final String guessedCharset) throws IOException, InterruptedException {
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
		try {
			writer.close();
		}
		catch (IOException shouldntHappen) {
			LOGGER.error("Interrupted while closing parallel output stream");
		}
		outputStream.close();
	}
}
