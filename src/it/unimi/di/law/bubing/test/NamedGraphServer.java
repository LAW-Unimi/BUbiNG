package it.unimi.di.law.bubing.test;

import it.unimi.dsi.lang.FlyweightPrototype;

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

//RELEASE-STATUS: DIST

/** A server that allows one to navigate through a graph whose nodes are decorated with names. */

public interface NamedGraphServer extends FlyweightPrototype<NamedGraphServer> {
	/** If <code>src</code> corresponds to the name of a node in the graph, this method returns
	 *  an array with the name of its successors (in some order); otherwise, it returns {@code null}.
	 *
	 * @param name the name of a node.
	 * @return the array of the successors of <code>src</code>, or {@code null} if <code>name</code>
	 * is not the name of a node.
	 */
	public CharSequence[] successors(final CharSequence name);
}
