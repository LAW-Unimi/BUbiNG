package it.unimi.di.law.bubing.test;

/*
 * Copyright (C) 2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.util.LiterallySignedStringMap;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Test;


public class NamedImmutableGraphServerTest {

	public static MutableString getFakeURL(final int i) {
		return new MutableString("http://www.foo" + i + ".bar/xxx/" + i + ".html");
	}

	@Test
	public void testSimple() throws IOException {
		int NUM_NODES = 100, NUM_QUERIES = 1000;
		MutableString nodeName[] = new MutableString[NUM_NODES];
		for (int i = 0; i < NUM_NODES; i++) nodeName[i] = getFakeURL(i);
		GOV3Function<MutableString> name2node = new GOV3Function.Builder<MutableString>().keys(new ObjectArrayList<>(nodeName)).transform(TransformationStrategies.iso()).build();
		ObjectArrayList<MutableString> node2name = new ObjectArrayList<>(nodeName);
		ArrayListMutableGraph graph = new ArrayListMutableGraph();
		graph.addNodes(NUM_NODES);
		for (int i = 0; i < NUM_NODES; i++)
			for (int j = i + 1; j < NUM_NODES; j++)
				if (i == 0 || j % i == 0) graph.addArc(i, j);
		ImmutableGraphNamedGraphServer burlServer = new ImmutableGraphNamedGraphServer(graph.immutableView(), new LiterallySignedStringMap(name2node, node2name));
		Random random = new Random(0);
		for (int query = 0; query < NUM_QUERIES; query++) {
			int i = random.nextInt(NUM_NODES);
			Set<MutableString> expectedSuccessorNames = new HashSet<>();
			for (int j = i + 1; j < NUM_NODES; j++)
				if (i == 0 || j % i == 0) expectedSuccessorNames.add(getFakeURL(j));

			CharSequence[] succBurls = burlServer.successors(getFakeURL(i));
			Set<CharSequence> successorNames = new HashSet<>();
			for (CharSequence succBurl: succBurls) successorNames.add(succBurl);
			assertEquals(expectedSuccessorNames, successorNames);
		}

	}
}
