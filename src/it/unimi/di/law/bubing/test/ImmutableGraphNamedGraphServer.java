package it.unimi.di.law.bubing.test;

/*
 * Copyright (C) 2010-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.util.StringMap;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

//RELEASE-STATUS: DIST

/** A {@link NamedGraphServer} using an {@link ImmutableGraph}
 * for the graph structure and a {@link StringMap} for the name of the nodes.
 *
 *
 * <p>Note that if you want to use the {@link #copy()} method and access copies
 * concurrently, the {@linkplain #ImmutableGraphNamedGraphServer(ImmutableGraph, StringMap) map provided at construction time} must be synchronized.
 */
public class ImmutableGraphNamedGraphServer implements NamedGraphServer {
	/** The underlying immutable graph. */
	private final ImmutableGraph graph;
	/** A string map for {@link #graph}'s URLs. */
	private StringMap<? extends CharSequence> map;
	/** {@link StringMap#list() <code>map</code>.list()}, cached. */
	private ObjectList<? extends CharSequence> list;

	/** Builds the server.
	 *
	 * @param graph the graph.
	 * @param map a string map representing the URLs of the graph.
	 */
	public ImmutableGraphNamedGraphServer(final ImmutableGraph graph, StringMap<? extends CharSequence> map) {
		if (graph.numNodes() != map.size()) throw new IllegalArgumentException("The graph has " + graph.numNodes() + " nodes, but the map has " + map.size() + " keys.");
		this.graph = graph;
		this.map = map;
		this.list = map.list();
	}

	@Override
	public CharSequence[] successors(final CharSequence name) {
		long srcNode = map.getLong(name);
		if (srcNode < 0) return null;
		int degree = graph.outdegree((int)srcNode);
		CharSequence result[] = new CharSequence[degree];
		final LazyIntIterator successors = graph.successors((int)srcNode);
		for (int s, i = 0; (s = successors.nextInt()) != -1; i++) result[i] = list.get(s);
		return result;
	}

	@Override
	public ImmutableGraphNamedGraphServer copy() {
		return new ImmutableGraphNamedGraphServer(graph.copy(), map);
	}
}
