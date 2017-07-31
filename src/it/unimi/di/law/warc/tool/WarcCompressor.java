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
 * Copyright (C) 2004-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
