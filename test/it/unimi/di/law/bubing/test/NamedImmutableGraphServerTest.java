package it.unimi.di.law.bubing.test;

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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.util.LiterallySignedStringMap;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;


public class NamedImmutableGraphServerTest {

	public static MutableString getFakeURL(final int i) {
		return new MutableString("http://www.foo" + i + ".bar/xxx/" + i + ".html");
	}

	@Test
	public void testSimple() throws IOException {
		final int NUM_NODES = 100, NUM_QUERIES = 1000;
		final MutableString nodeName[] = new MutableString[NUM_NODES];
		for (int i = 0; i < NUM_NODES; i++) nodeName[i] = getFakeURL(i);
		final GOV3Function<MutableString> name2node = new GOV3Function.Builder<MutableString>().keys(new ObjectArrayList<>(nodeName)).transform(TransformationStrategies.iso()).build();
		final ObjectArrayList<MutableString> node2name = new ObjectArrayList<>(nodeName);
		final ArrayListMutableGraph graph = new ArrayListMutableGraph();
		graph.addNodes(NUM_NODES);
		for (int i = 0; i < NUM_NODES; i++)
			for (int j = i + 1; j < NUM_NODES; j++)
				if (i == 0 || j % i == 0) graph.addArc(i, j);
		final ImmutableGraphNamedGraphServer burlServer = new ImmutableGraphNamedGraphServer(graph.immutableView(), new LiterallySignedStringMap(name2node, node2name));
		final Random random = new Random(0);
		for (int query = 0; query < NUM_QUERIES; query++) {
			final int i = random.nextInt(NUM_NODES);
			final Set<MutableString> expectedSuccessorNames = new HashSet<>();
			for (int j = i + 1; j < NUM_NODES; j++)
				if (i == 0 || j % i == 0) expectedSuccessorNames.add(getFakeURL(j));

			final CharSequence[] succBurls = burlServer.successors(getFakeURL(i));
			final Set<CharSequence> successorNames = new HashSet<>();
			for (final CharSequence succBurl: succBurls) successorNames.add(succBurl);
			assertEquals(expectedSuccessorNames, successorNames);
		}

	}
}
