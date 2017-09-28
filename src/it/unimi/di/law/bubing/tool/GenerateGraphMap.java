package it.unimi.di.law.bubing.tool;

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

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/** Builds and saves the graph map, that is, a text file containing all URLs ever crawled, and a binary file containing the corresponding
 * nodes (duplicates are mapped to their archetype position).
 *
 * <p>The input format for the tool are a number of TAB-separated files (one per store), each containing
 * triples &lt;URL,digest,position&gt;, which are assumed to be stably reverse-sorted starting at the digest (the position
 * is the <strong>local final</strong> position). The positions are set to -1 in correspondence to duplicate pages.
 *
 * <p>The output is given by a text URL list, a corresponding binary list of long values (the node assigned to each URL)
 * and a list of nodes and corresponding archetype URL, TAB separated.
 *
 * <p>This tool will print on standard output the number of nodes of the resulting graph.
 */

//RELEASE-STATUS: DIST

public class GenerateGraphMap {

	@SuppressWarnings("resource")
	public static void main(String[] arg) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(GenerateGraphMap.class.getName(), "Generates a graph map, and prints on standard output the number of nodes of the graph.",
				new Parameter[] {
				new FlaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "input", "An input file.").setAllowMultipleDeclarations(true),
				new UnflaggedOption("urls", JSAP.STRING_PARSER, JSAP.NOT_REQUIRED, "The name of the file that will contain all URLs."),
				new UnflaggedOption("nodes", JSAP.STRING_PARSER, JSAP.NOT_REQUIRED, "The name of the binary file that will contain the corresponding nodes."),
				new UnflaggedOption("nonDuplicateUrls", JSAP.STRING_PARSER, JSAP.NOT_REQUIRED, "The name of the file that will contain non-duplicate URLs, prefixed with their node and followed by their status line.")
		});

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;

		final MutableString s = new MutableString();
		final ProgressLogger pl = new ProgressLogger();
		long maxGlobalPosition = -1;

		final PrintStream urls = new PrintStream(new FileOutputStream(jsapResult.getString("urls")), false, "US-ASCII");
		final PrintStream nonDuplicateUrls = new PrintStream(new FileOutputStream(jsapResult.getString("nonDuplicateUrls")), false, "US-ASCII");
		final DataOutputStream positions = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(jsapResult.getString("nodes"))));

		for(String input: jsapResult.getStringArray("input")) {
			final FastBufferedReader fastBufferedReader = new FastBufferedReader(new InputStreamReader(new FileInputStream(input), Charsets.US_ASCII));

			final long globalOffset = maxGlobalPosition + 1;
			pl.itemsName = "lines";
			pl.start("Reading... ");

			long lastLocalFinalPosition = Long.MIN_VALUE;
			for(long line = 0; fastBufferedReader.readLine(s) != null; line++) {
				try {
					final int firstTab = s.indexOf('\t');
					final int secondTab = s.indexOf('\t', firstTab + 1);
					final int thirdTab = s.indexOf('\t', secondTab + 1);
					final long localFinalPosition = Long.parseLong(new String(s.array(), secondTab + 1, thirdTab - secondTab - 1));
					final MutableString url = s.substring(0, firstTab);
					final MutableString status = s.substring(thirdTab + 1);
					url.println(urls);
					if (localFinalPosition != -1) {
						lastLocalFinalPosition = localFinalPosition;
						if (lastLocalFinalPosition + globalOffset > maxGlobalPosition) maxGlobalPosition = lastLocalFinalPosition + globalOffset;
						nonDuplicateUrls.print(lastLocalFinalPosition + globalOffset);
						nonDuplicateUrls.print('\t');
						url.print(nonDuplicateUrls);
						nonDuplicateUrls.print('\t');
						status.println(nonDuplicateUrls);
						assert status.indexOf('\n') < 0; // No newlines in the status line
					}
					positions.writeLong(lastLocalFinalPosition + globalOffset);
					pl.lightUpdate();
				}
				catch(Exception e) {
					System.err.println("Exception at line " + line);
					throw new RuntimeException(e);
				}
			}
			pl.done();

			fastBufferedReader.close();
		}

		urls.close();
		nonDuplicateUrls.close();
		positions.close();
		System.out.println(maxGlobalPosition + 1);
	}
}
