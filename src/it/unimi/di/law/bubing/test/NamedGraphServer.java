package it.unimi.di.law.bubing.test;

import it.unimi.dsi.lang.FlyweightPrototype;

/*
 * Copyright (C) 2010-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
