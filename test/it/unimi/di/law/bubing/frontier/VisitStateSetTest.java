package it.unimi.di.law.bubing.frontier;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

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

}
