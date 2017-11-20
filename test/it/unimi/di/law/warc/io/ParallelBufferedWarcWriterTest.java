package it.unimi.di.law.warc.io;

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

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.RandomTestMocks;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

//RELEASE-STATUS: DIST

public class ParallelBufferedWarcWriterTest {
	public static final boolean DEBUG = false;

	private final static Logger LOGGER = LoggerFactory.getLogger(ParallelBufferedWarcWriterTest.class);

	final static int NUM_THREADS = 4;
	final static int NUM_RECORDS = 100 * NUM_THREADS;
	final static int MAX_NUMBER_OF_HEADERS = 50;
	final static int MAX_LENGTH_OF_HEADER = 20;
	final static int MAX_LENGTH_OF_BODY = 10 * 1024;

	@Test
	public void testRecord() throws IOException, InterruptedException, URISyntaxException {

		for(boolean gzip: new boolean[] { true, false }) {
			final FastByteArrayOutputStream out = new FastByteArrayOutputStream();
			final ParallelBufferedWarcWriter warcParallelOutputStream = new ParallelBufferedWarcWriter(out, gzip);
			final Thread thread[] = new Thread[NUM_THREADS];

			final URI fakeUri = new URI("http://this.is/a/fake");
			final RandomTestMocks.HttpResponse[] response = new RandomTestMocks.HttpResponse[NUM_RECORDS];
			for(int i = 0; i < NUM_THREADS; i++)
				(thread[i] = new Thread(Integer.toString(i)) {
					@Override
					public void run() {
						final int index = Integer.parseInt(getName());
						for (int i = index * (NUM_RECORDS / NUM_THREADS); i < (index + 1) * (NUM_RECORDS / NUM_THREADS); i++) {
							try {
								response[i] = new RandomTestMocks.HttpResponse(MAX_NUMBER_OF_HEADERS, MAX_LENGTH_OF_HEADER, MAX_LENGTH_OF_BODY, i);
								HttpResponseWarcRecord record = new HttpResponseWarcRecord(fakeUri, response[i]);
								warcParallelOutputStream.write(record);
								LOGGER.info("Thread " + index + " wrote record " + i);
							} catch(Exception e) { throw new RuntimeException(e); }
						}
					}
				}).start();


			for(Thread t: thread) t.join();
			warcParallelOutputStream.close();
			out.close();

			final FastBufferedInputStream in = new FastBufferedInputStream(new FastByteArrayInputStream(out.array, 0, out.length));
			WarcReader reader = gzip ? new CompressedWarcReader(in) : new UncompressedWarcReader(in);

			final boolean found[] = new boolean[NUM_RECORDS];
			for (int i = 0; i < NUM_RECORDS; i++) {
				final HttpResponseWarcRecord r = (HttpResponseWarcRecord) reader.read();
				final int pos = Integer.parseInt(r.getFirstHeader("Position").getValue());
				found[pos] = true;
				assertArrayEquals(ByteStreams.toByteArray(response[pos].getEntity().getContent()), ByteStreams.toByteArray(r.getEntity().getContent()));
			}
			in.close();

			for(int i = NUM_RECORDS; i-- != 0;) assertTrue(Integer.toString(i), found[i]);
		}
	}
}
