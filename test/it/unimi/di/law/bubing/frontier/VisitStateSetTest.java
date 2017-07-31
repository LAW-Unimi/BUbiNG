package it.unimi.di.law.bubing.frontier;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;

//RELEASE-STATUS: DIST

public class VisitStateSetTest {

	@Test
	public void testAddRemove() {
		VisitState a = new VisitState(null, "a".getBytes(Charsets.ISO_8859_1));
		VisitState b = new VisitState(null, "b".getBytes(Charsets.ISO_8859_1));
		VisitStateSet s = new VisitStateSet();

		assertTrue(s.add(a));
		assertFalse(s.add(a));
		assertTrue(s.add(b));
		assertFalse(s.add(b));
		assertSame(a, s.get(a.schemeAuthority));
		assertSame(a, s.get("a".getBytes(Charsets.ISO_8859_1)));
		assertSame(b, s.get(b.schemeAuthority));
		assertSame(b, s.get("b".getBytes(Charsets.ISO_8859_1)));
	}

	@Test
	public void testResize() {
		final VisitState[] visitState = new VisitState[2000];
		for(int i = visitState.length; i-- != 0;) visitState[i] = new VisitState(null, Integer.toString(i).getBytes(Charsets.ISO_8859_1));

		VisitStateSet s = new VisitStateSet();
		for(int i = 2000; i-- != 0;) assertTrue(s.add(visitState[i]));
		assertEquals(2000, s.size());
		for(int i = 2000; i-- != 0;) assertFalse(s.add(new VisitState(null, Integer.toString(i).getBytes(Charsets.ISO_8859_1))));
		for(int i = 1000; i-- != 0;) assertTrue(s.remove(visitState[i]));

		for(int i = 1000; i-- != 0;) assertFalse(s.remove(visitState[i]));
		for(int i = 1000; i-- != 0;) assertNull(s.get(Integer.toString(i).getBytes(Charsets.ISO_8859_1)));
		for(int i = 2000; i-- != 1000;) assertSame(visitState[i], s.get(Integer.toString(i).getBytes(Charsets.ISO_8859_1)));
		assertEquals(1000, s.size());
		assertFalse(s.isEmpty());
		s.clear();
		assertEquals(0, s.size());
		assertTrue(s.isEmpty());
		s.clear();
		assertEquals(0, s.size());
		assertTrue(s.isEmpty());
	}

	@Ignore
	@Test
	public void testSerialization() {
		// TODO
	}
}
