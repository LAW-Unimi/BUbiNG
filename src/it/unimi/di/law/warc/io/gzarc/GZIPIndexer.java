package it.unimi.di.law.warc.io.gzarc;

/*
 * Copyright (C) 2004-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

// RELEASE-STATUS: DIST

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

public class GZIPIndexer {
	private static final Logger LOGGER = LoggerFactory.getLogger(GZIPIndexer.class);

	/**
	 * Returns a list of pointers to a GZIP archive entries positions (including the end of file).
	 *
	 * @param in the stream from which to read the GZIP archive.
	 * @return a list of longs where the <em>i</em>-th long is the offset in the stream of the first byte of the <em>i</em>-th archive entry.
	 */
	public static LongBigArrayBigList index(final InputStream in) throws IOException {
		return index(in, null);
	}

	/**
	 * Returns a list of pointers to a GZIP archive entries positions (including the end of file).
	 *
	 * @param in the stream from which to read the GZIP archive.
	 * @param pl a progress logger.
	 * @return a list of longs where the <em>i</em>-th long is the offset in the stream of the first byte of the <em>i</em>-th archive entry.
	 */
	public static LongBigArrayBigList index(final InputStream in, final ProgressLogger pl) throws IOException {
		final LongBigArrayBigList pointers = new LongBigArrayBigList();
		long current = 0;
		final GZIPArchiveReader gzar = new GZIPArchiveReader(in);
		GZIPArchive.ReadEntry re;
		for (;;) {
			re = gzar.skipEntry();
			if (re == null) break;
			pointers.add(current);
			current += re.compressedSkipLength;
			if (pl != null) pl.lightUpdate();
		}
		in.close();
		return pointers;
	}

	public static void main(String[] arg) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(GZIPIndexer.class.getName(), "Computes and stores a quasi-succinct index for a compressed archive.",
				new Parameter[] {
					new UnflaggedOption("archive", JSAP.STRING_PARSER, JSAP.REQUIRED, "The name a GZIP's archive."),
					new UnflaggedOption("index", JSAP.STRING_PARSER, JSAP.REQUIRED, "The output (a serialized LongBigList of pointers to the records in the archive) filename."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;

		final FastBufferedInputStream input = new FastBufferedInputStream(new FileInputStream(jsapResult.getString("archive")));
		ProgressLogger pl = new ProgressLogger(LOGGER, 1, TimeUnit.MINUTES, "records");
		pl.start("Scanning...");
		final EliasFanoMonotoneLongBigList list = new EliasFanoMonotoneLongBigList(index(input, pl));
		pl.done();
		BinIO.storeObject(list, jsapResult.getString("index"));
	}
}

