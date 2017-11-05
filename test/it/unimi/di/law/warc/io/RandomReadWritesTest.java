package it.unimi.di.law.warc.io;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.HttpEntity;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

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

import it.unimi.di.law.warc.io.gzarc.GZIPIndexer;
import it.unimi.di.law.warc.records.HttpRequestWarcRecord;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.RandomTestMocks;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.BufferedHttpEntityFactory;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.logging.ProgressLogger;

public class RandomReadWritesTest {

	private final static Logger LOGGER = LoggerFactory.getLogger(RandomReadWritesTest.class);

	final static int RND_RECORDS = 100;
	final static int MAX_NUMBER_OF_HEADERS = 50;
	final static int MAX_LENGTH_OF_HEADER = 20;
	final static int MAX_LENGTH_OF_BODY = 10 * 1024;
	final static float RESPONSE_PROBABILITY = 0.7f;

	final static int TEST_RECORDS = 200;

	private WarcRecord[] randomRecords;

	@Before
	public void init() throws IOException {
		randomRecords = prepareRndRecords();
	}

	public static WarcRecord[] prepareRndRecords() throws IOException {
		return prepareRndRecords(RND_RECORDS, RESPONSE_PROBABILITY, MAX_NUMBER_OF_HEADERS, MAX_LENGTH_OF_HEADER, MAX_LENGTH_OF_BODY);
	}

	public static WarcRecord[] prepareRndRecords(final int numRecords, final float responseProbability) throws IOException {
		return prepareRndRecords(numRecords, responseProbability,  MAX_NUMBER_OF_HEADERS, MAX_LENGTH_OF_HEADER, MAX_LENGTH_OF_BODY);
	}

	public static WarcRecord[] prepareRndRecords(final int numRecords, final float responseProbability, final int maxNumberOfHeaders, final int maxLenghtOfHeader, final int maxLengthOfBody) throws IOException {
		final WarcRecord[] randomRecords = new WarcRecord[numRecords];
		URI fakeUri = null;
		try {
			fakeUri = new URI("http://this.is/a/fake");
		} catch (final URISyntaxException ignored) {}
		for (int pos = 0; pos < numRecords; pos++) {
			if (RandomTestMocks.RNG.nextFloat() < responseProbability)
				randomRecords[pos] = new HttpResponseWarcRecord(fakeUri, new RandomTestMocks.HttpResponse(maxNumberOfHeaders, maxLenghtOfHeader, maxLengthOfBody, pos), BufferedHttpEntityFactory.INSTANCE);
			else
				randomRecords[pos] = new HttpRequestWarcRecord(fakeUri, new RandomTestMocks.HttpRequest(maxNumberOfHeaders, maxLenghtOfHeader, pos));
		}
		return randomRecords;
	}

	@SuppressWarnings("resource")
	public static int[] writeRecords(final String path, final int numRecords, final WarcRecord[] randomRecords, final int parallel) throws IOException, InterruptedException {
		final ProgressLogger pl = new ProgressLogger(LOGGER, "records");
		if (parallel <= 1) pl.expectedUpdates = numRecords;
		final ProgressLogger plb = new ProgressLogger(LOGGER, "KB");
		final CountingOutputStream cos = new CountingOutputStream(new FastBufferedOutputStream(new FileOutputStream (path)));
		final WarcWriter ww;
		if (parallel == 0) {
			ww = new UncompressedWarcWriter(cos);
			pl.start("Writing records…");
		} else if (parallel == 1) {
			ww = new CompressedWarcWriter(cos);
			pl.start("Writing records (compressed)…");
		} else {
			ww = null;
			pl.start("SHOULD NOT HAPPEN");
			throw new IllegalStateException();
		}
		plb.start();
		long written = 0;
		final int[] position = new int[numRecords];
		for (int i = 0; i < numRecords; i++) {
			final int pos = RandomTestMocks.RNG.nextInt(randomRecords.length);
			position[i] = pos;
			ww.write(randomRecords[pos]);
			if (parallel <= 0) {
				pl.lightUpdate();
				plb.update((cos.getCount() - written) / 1024);
			}
			written = cos.getCount();
		}
		ww.close();
		pl.done(numRecords);
		plb.done(cos.getCount());
		return position;
	}

	protected static int getPosition(final WarcRecord record) {
		int pos;
		if (record instanceof HttpResponseWarcRecord) {
			final HttpResponseWarcRecord response = (HttpResponseWarcRecord)record;
			pos = Integer.parseInt(response.getFirstHeader("Position").getValue());
		} else
			pos = Integer.parseInt(((HttpRequestWarcRecord)record).getFirstHeader("Position").getValue());
		return pos;
	}

	protected static void readRecords(final String path, final int[] position, final int maxLengthOfBody, final boolean readFully, final boolean compress) throws IOException {
		final ProgressLogger pl = new ProgressLogger(LOGGER, "records");
		final ProgressLogger plb = new ProgressLogger(LOGGER, "KB");
		final CountingInputStream cis = new CountingInputStream(new FileInputStream(path));
		final WarcReader wr = compress ? new CompressedWarcReader(cis) : new UncompressedWarcReader(cis);
		pl.start("Reading records" + (readFully ? " (fully)" : "") + (compress ? " (compressed)…" : "…"));
		plb.start();
		long read = 0;
		final byte[] body = new byte[maxLengthOfBody];
		for (int i = 0 ;; i++) {
			final WarcRecord record = wr.read();
			if (record == null) break;
			if (readFully) {
				final int pos = getPosition(record);
				if (record instanceof HttpResponseWarcRecord) {
					final HttpResponseWarcRecord response = (HttpResponseWarcRecord)record;
					final HttpEntity entity = response.getEntity();
					ByteStreams.readFully(entity.getContent(), body, 0, (int)entity.getContentLength());
				}
				assert position[i] == pos : "At position " + i + " expected record " + position[i] + ", but found record " + pos;
			}
			pl.lightUpdate();
			plb.update((cis.getCount() - read) / 1024);
			read = cis.getCount();
		}
		pl.done();
		plb.done();
		cis.close();
	}

	@Test
	public void testUncompressedWritesReads() throws IOException, InterruptedException {
		final String path = "/tmp/random.warc";
		final int[] sequence = writeRecords(path, TEST_RECORDS, randomRecords, 0);
		readRecords(path, sequence, MAX_LENGTH_OF_BODY, true, false);
	}

	@Test
	public void testCompressedWritesReads() throws IOException, InterruptedException {
		final String path = "/tmp/random.warc.gz";
		final int[] sequence = writeRecords(path, TEST_RECORDS, randomRecords, 1);
		readRecords(path, sequence, MAX_LENGTH_OF_BODY, true, true);
	}

	@Test
	public void testCompressedIndexedReads() throws IOException, InterruptedException {
		final String path = "/tmp/random.warc.gz";
		final int[] sequence = writeRecords(path, TEST_RECORDS, randomRecords, 1);
		final LongBigArrayBigList index = GZIPIndexer.index(new FileInputStream(path));
		final FastBufferedInputStream input = new FastBufferedInputStream(new FileInputStream(path));
		final WarcReader wr = new CompressedWarcReader(input);
		for (int i = (int)index.size64() - 1; i >= 0 ; i--) {
			wr.position(index.getLong(i));
			final WarcRecord r = wr.read();
			final int pos = getPosition(r);
			System.err.println(sequence[i] + " DIOCANE " + pos);
			assert sequence[i] == pos : "At position " + i + " expected record " + sequence[i] + ", but found record " + pos;
			System.err.println(pos);
		}
		readRecords(path, sequence, MAX_LENGTH_OF_BODY, true, true);
	}

	@Test
	public void testCompressedCachedIndexedReads() throws IOException, InterruptedException {
		final String path = "/tmp/random.warc.gz";
		final int[] sequence = writeRecords(path, TEST_RECORDS, randomRecords, 1);
		final LongBigArrayBigList index = GZIPIndexer.index(new FileInputStream(path));
		final FastBufferedInputStream input = new FastBufferedInputStream(new FileInputStream(path));
		final WarcCachingReader cwr = new CompressedWarcCachingReader(input);
		for (int i = (int)index.size64() - 1; i >= 0 ; i--) {
			cwr.position(index.getLong(i));
			final WarcRecord r = cwr.cache().read();
			final int pos = getPosition(r);
			assert sequence[i] == pos : "At position " + i + " expected record " + sequence[i] + ", but found record " + pos;
		}
		readRecords(path, sequence, MAX_LENGTH_OF_BODY, true, true);
	}

	public static void main(String[] args) throws JSAPException, IOException, InterruptedException {
		final SimpleJSAP jsap = new SimpleJSAP(RandomReadWritesTest.class.getName(), "Writes some random records on disk.",
			new Parameter[] {
				new FlaggedOption("random", JSAP.INTEGER_PARSER, "100", JSAP.NOT_REQUIRED, 'r', "random", "The number of random record to sample from."),
				new FlaggedOption("body", JSAP.INTSIZE_PARSER, "4K", JSAP.NOT_REQUIRED, 'b', "body", "The maximum size of the random generated body (in bytes)."),
				new Switch("fully", 'f', "fully", "Whether to read fully the record (and do a minimal sequential cosnsistency check)."),
				new Switch("writeonly", 'w', "writeonly", "Whether to skip the read part (if present, 'fully' will be ignored."),
				new UnflaggedOption("path", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The path to write to."),
				new UnflaggedOption("records", JSAP.INTSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The numer of records to write."),
		});

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final String path = jsapResult.getString("path");
		final boolean compress = path.endsWith(".gz");
		final boolean fully = jsapResult.getBoolean("fully");
		final int parallel = compress ? 1 : 0;

		final int body = jsapResult.getInt("body");

		final WarcRecord[] rnd = prepareRndRecords(jsapResult.getInt("random"), RESPONSE_PROBABILITY, MAX_NUMBER_OF_HEADERS, MAX_LENGTH_OF_HEADER, body);
		final int[] sequence = writeRecords(path, jsapResult.getInt("records"), rnd, parallel);
		if (! jsapResult.getBoolean("writeonly"))
			readRecords(path, sequence, body, fully, compress);

	}
}
