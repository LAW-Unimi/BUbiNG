package it.unimi.di.law.warc.tool;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.di.law.warc.io.CompressedWarcWriter;
import it.unimi.di.law.warc.io.UncompressedWarcReader;
import it.unimi.di.law.warc.io.WarcReader;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.di.law.warc.io.gzarc.GZIPArchive;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.logging.ProgressLogger;

/**
 * A tool to compress a WARC file adding the extra GZIP header fields required
 * by the {@link GZIPArchive} format used by compressed WARC files.
 */
public class WarcCompressor {

    private static final Logger LOGGER = LoggerFactory.getLogger(WarcCompressor.class);

    public static void main(String arg[]) throws IOException, InterruptedException, JSAPException {

	SimpleJSAP jsap = new SimpleJSAP(WarcCompressor.class.getName(),
		"Given a store uncompressed, write a compressed store.",
		new Parameter[] { new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "output", "The output filename  (- for stdout)."),
		new UnflaggedOption("store", JSAP.STRING_PARSER, JSAP.NOT_REQUIRED, "The name of the store (if omitted, stdin)."),
	});

	JSAPResult jsapResult = jsap.parse(arg);
	if (jsap.messagePrinted()) return;

	final InputStream in = jsapResult.userSpecified("store") ? new FastBufferedInputStream(new FileInputStream(jsapResult.getString("store"))) : System.in;

	final WarcReader reader = new UncompressedWarcReader(in);
	final ProgressLogger pl = new ProgressLogger(LOGGER, 1, TimeUnit.MINUTES, "records");
	final String output = jsapResult.getString("output");

	PrintStream out = "-".equals(output) ? System.out : new PrintStream(new FastBufferedOutputStream(new FileOutputStream(output)), false, "UTF-8");
	final WarcWriter writer = new CompressedWarcWriter(out);

	pl.itemsName = "records";
	pl.displayFreeMemory = true;
	pl.displayLocalSpeed = true;
	pl.start("Scanning...");

	for (long storePosition = 0;; storePosition++) {
	    LOGGER.trace("STOREPOSITION " + storePosition);
	    WarcRecord record = null;
	    try {
		record = reader.read();
	    } catch (Exception e) {
		LOGGER.error("Exception while reading record " + storePosition + " ");
		LOGGER.error(e.getMessage());
		e.printStackTrace();
		continue;
	    }
	    if (record == null)
		break;
	    writer.write(record);
	    pl.lightUpdate();
	}
	pl.done();
	writer.close();
    }

}
