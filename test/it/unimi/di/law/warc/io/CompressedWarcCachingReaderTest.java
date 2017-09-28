package it.unimi.di.law.warc.io;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;

import it.unimi.di.law.warc.io.gzarc.GZIPArchive.FormatException;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcRecord;

public class CompressedWarcCachingReaderTest {

	final static int TEST_RECORDS = 200;
	final static String PATH0 = "/tmp/warc0.gz";
	final static String PATH1 = "/tmp/warc1.gz";

	static int[] position;

	@BeforeClass
	public static void init() throws IOException, InterruptedException {
		WarcRecord[] randomRecords = RandomReadWritesTest.prepareRndRecords();
		position = RandomReadWritesTest.writeRecords(PATH0, TEST_RECORDS, randomRecords, 1); // 1 stands for compressed!
		randomRecords = RandomReadWritesTest.prepareRndRecords(2, 1);
		RandomReadWritesTest.writeRecords(PATH1, TEST_RECORDS, randomRecords, 1); // 1 stands for compressed!
	}

	@Test
	public void sequentialReads() throws WarcFormatException, FormatException, IOException {
		FileInputStream input = new FileInputStream(PATH0);
		CompressedWarcCachingReader cwc = new CompressedWarcCachingReader(input);
		WarcReader wr;
		int i = 0;
		while ((wr = cwc.cache()) != null) {
			assertEquals(position[i], RandomReadWritesTest.getPosition(wr.read()));
			i++;
		}
		input.close();
	}

	@SuppressWarnings("unused")
	private static void consumeRecord(final WarcRecord r) throws IllegalStateException, IOException {
		if (r instanceof it.unimi.di.law.warc.records.HttpResponseWarcRecord) {
			final HttpEntity entity = ((it.unimi.di.law.warc.records.HttpResponseWarcRecord)r).getEntity();
			final InputStream is = entity.getContent();
			final String throwAway = IOUtils.toString(is, Charsets.ISO_8859_1);
			is.close();
		} else {
			 final Header[] headers = ((it.unimi.di.law.warc.records.HttpRequestWarcRecord)r).getAllHeaders();
			 final String throwAway = Arrays.toString(headers);
		}
	}

	@Test
	public void idempotentReads() throws WarcFormatException, FormatException, IOException {
		FileInputStream input = new FileInputStream(PATH0);
		CompressedWarcCachingReader cwc = new CompressedWarcCachingReader(input);
		WarcReader wr;
		int i = 0;
		while ((wr = cwc.cache()) != null) {
			WarcRecord r = wr.read();
			consumeRecord(r);
			assertEquals(position[i], RandomReadWritesTest.getPosition(r));
			r = wr.read();
			consumeRecord(r);
			i++;
		}
		input.close();
	}

	@Test
	public void randomReads() throws WarcFormatException, FormatException, IOException {
		FileInputStream input = new FileInputStream(PATH0);
		CompressedWarcCachingReader cwc = new CompressedWarcCachingReader(input);
		final ArrayList<WarcReader> cache = new ArrayList<>(position.length);
		WarcReader wr;
		while ((wr = cwc.cache()) != null) cache.add(wr);
		assertEquals(cache.size(), position.length);
		Collections.shuffle(cache);
		int[] result = new int[position.length];
		int i = 0;
		for (WarcReader rwr : cache) {
			result[i++] = RandomReadWritesTest.getPosition(rwr.read());
		}
		Arrays.sort(result);
		int[] sortedPosition = new int[position.length];
		System.arraycopy(position, 0, sortedPosition, 0, position.length);
		Arrays.sort(sortedPosition);
		assertArrayEquals(sortedPosition, result);
		input.close();
	}

	@Test
	public void parallelReadsSameThread() throws WarcFormatException, FormatException, IOException {
		FileInputStream input = new FileInputStream(PATH1);
		CompressedWarcCachingReader cwc = new CompressedWarcCachingReader(input);
		byte[] c0 = IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent());
		byte[] c1 = IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent());
		input.close();

		input = new FileInputStream(PATH1);
		cwc = new CompressedWarcCachingReader(input);
		InputStream i0 = ((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent();
		InputStream i1 = ((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent();

		final int l = Math.min(c0.length, c1.length);
		for (int i = 0; i < l; i++) {
			int b0 = i0.read();
			int b1 = i1.read();
			assertTrue(b0 >= 0);
			assertTrue(b1 >= 0);
			assertEquals(b0, c0[i] & 0xFF);
			assertEquals(b1, c1[i] & 0xFF);
		}

		input.close();
	}

	@Test
	public void parallelReadsManyThreads() throws Throwable {
		FileInputStream input = new FileInputStream(PATH1);
		CompressedWarcCachingReader cwc = new CompressedWarcCachingReader(input);
		byte[][] c = { IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent()),
			IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent()),
			IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent()),
			IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent()),
			IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent()),
			IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent()),
			IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent()),
			IOUtils.toByteArray(((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent()),
		};
		input.close();

		input = new FileInputStream(PATH1);
		cwc = new CompressedWarcCachingReader(input);
		InputStream[] is = { ((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent(),
				((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent(),
				((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent(),
				((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent(),
				((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent(),
				((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent(),
				((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent(),
				((HttpResponseWarcRecord)cwc.cache().read()).getEntity().getContent(),
		};

		Thread[] thread = new Thread[8];
		final Throwable[] throwable = new Throwable[1];
		for(int u = 0; u < 8; u++) {
			final int t = u;
			thread[t] = new Thread() {
				@Override
				public void run() {
					final int l = c[t].length;
					for (int i = 0; i < l; i++) {
						int b;
						try {
							b = is[t].read();
							assertTrue(b >= 0);
							assertEquals(b, c[t][i] & 0xFF);
						} catch (Throwable t) {
							throwable[0] = t;
							throw new RuntimeException(t.getMessage(), t);
						}
					}
				}
			};
		};

		for(int t = 0; t < 8; t++) thread[t].start();
		for(int t = 0; t < 8; t++) thread[t].join();
		if (throwable[0] != null) throw throwable[0];
		input.close();
	}

}
