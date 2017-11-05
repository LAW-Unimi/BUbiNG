package it.unimi.di.law.bubing.tool;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Properties;

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

import it.unimi.dsi.big.webgraph.EFGraph;
import it.unimi.dsi.big.webgraph.EFGraph.LongWordOutputBitStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.util.ByteBufferLongBigList;

/** Concatenates {@linkplain EFGraph Elias&ndash;Fano graphs}.
 *
 * <p>This tool creates an EF graph by concatenating the bit streams of several {@linkplain EFGraph Elias&ndash;Fano graphs}.
 * For the process being meaningful, the upper bound used in each graph must be the same.
 *
 * <p>The intended usage of this tool is a distributed graph construction procedure: segments of
 * contiguous successor lists can be generated independently, and concatenated afterwards.
 */

//RELEASE-STATUS: DIST

public class CatEFGraphs {

	public static void main(String[] arg) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(CatEFGraphs.class.getName(), "Concatenates EF graphs with a common upper bound.",
				new Parameter[] {
					new Switch("mapped", 'm', "mapped", "Memory-map the input graphs instead of loading them in core memory."),
					new UnflaggedOption("output", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the concatenated EF graph."),
					new UnflaggedOption("input", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.GREEDY, "The basenames of the EF graphs to concatenate."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;

		final String output = jsapResult.getString("output");
		final boolean mapped = jsapResult.userSpecified("mapped");

		@SuppressWarnings("resource")
		final WritableByteChannel outputGraphChannel = new FileOutputStream(output + EFGraph.GRAPH_EXTENSION).getChannel();
		final LongWordOutputBitStream outputGraphStream = new EFGraph.LongWordOutputBitStream(outputGraphChannel, ByteOrder.nativeOrder());
		final OutputBitStream outputOffsets = new OutputBitStream(output + EFGraph.OFFSETS_EXTENSION);

		boolean first = true;

		Properties properties = new Properties(); // Dummy, just to avoid the null pointer
		long numNodes = 0;
		long numArcs = 0;
		long upperBound = -1;
		for(final String input: jsapResult.getStringArray("input")) {
			final FileInputStream propertyFile = new FileInputStream(input + EFGraph.PROPERTIES_EXTENSION);
			properties = new Properties();
			properties.load(propertyFile);
			propertyFile.close();

			final ByteOrder byteOrder;
			if (properties.get("byteorder").equals(ByteOrder.BIG_ENDIAN.toString())) byteOrder = ByteOrder.BIG_ENDIAN;
			else if (properties.get("byteorder").equals(ByteOrder.LITTLE_ENDIAN.toString())) byteOrder = ByteOrder.LITTLE_ENDIAN;
			else throw new IllegalArgumentException("Unknown byte order " + properties.get("byteorder"));


			final long n = Long.parseLong(properties.getProperty("nodes"));
			numNodes += n;
			numArcs += Long.parseLong(properties.getProperty("arcs"));

			final long ub = properties.containsKey("upperbound") ? Long.parseLong(properties.getProperty("upperbound")) : n;
			if (upperBound < 0) upperBound = ub;
			else if (upperBound != ub) throw new IllegalArgumentException(input + " upper bound " + ub + " != " + upperBound);
			final InputBitStream inputOffsets = new InputBitStream(input + EFGraph.OFFSETS_EXTENSION);

			if (! first) inputOffsets.readDelta();
			long length = 0;
			for(long i = first ? 0 : 1; i <= n; i++) {
				final long delta = inputOffsets.readLongDelta();
				outputOffsets.writeLongDelta(delta);
				length += delta;
			}

			inputOffsets.close();

			if (mapped) {
				final FileInputStream inputStream = new FileInputStream(input + EFGraph.GRAPH_EXTENSION);
				outputGraphStream.append(ByteBufferLongBigList.map(inputStream.getChannel(), byteOrder), length);
				inputStream.close();
			} // TODO: eliminate the dependency from the standard EFGraph upon new release
			else outputGraphStream.append(it.unimi.dsi.webgraph.EFGraph.loadLongBigList(input + EFGraph.GRAPH_EXTENSION, byteOrder), length);

			first = false;
		}

		properties.setProperty("nodes", Long.toString(numNodes));
		properties.setProperty("arcs", Long.toString(numArcs));
		properties.setProperty("byteorder", ByteOrder.nativeOrder().toString());
		final FileOutputStream outputProperties = new FileOutputStream(output + EFGraph.PROPERTIES_EXTENSION);
		properties.store(outputProperties, "EFGraph properties");
		outputProperties.close();

		outputOffsets.close();
		outputGraphStream.close();
	}
}
