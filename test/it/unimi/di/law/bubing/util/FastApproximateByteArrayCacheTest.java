package it.unimi.di.law.bubing.util;

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

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.base.Charsets;

public class FastApproximateByteArrayCacheTest {
	@Test
	public void test() {
		FastApproximateByteArrayCache cache = new FastApproximateByteArrayCache(1000, 4);
		assertTrue(cache.add("A".getBytes(Charsets.ISO_8859_1)));
		assertFalse(cache.add("A".getBytes(Charsets.ISO_8859_1)));
		for(int i = 0; i <  1000; i++) cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1));
		assertTrue(cache.add("A".getBytes(Charsets.ISO_8859_1)));
	}

	@Test
	public void testMany() {
		FastApproximateByteArrayCache cache = new FastApproximateByteArrayCache(128L * 1024 * 8 * 4, 4);
		for(int i = 0; i < (int)(128 * 1024 * .70); i++) assertTrue(Integer.toBinaryString(i), cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
		for(int i = 0; i < (int)(128 * 1024 * .70); i++) assertFalse(Integer.toBinaryString(i), cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
		for(int i = (int)(128 * 1024 * .70); i < 128 * 1024; i++) assertTrue(Integer.toBinaryString(i), cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
		for(int i = 0; i < 128; i++) assertTrue(cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
	}

	@Test
	public void testMany2() {
		FastApproximateByteArrayCache cache = new FastApproximateByteArrayCache(128L * 1024 * 8 * 4, 4);
		for(int i = 0; i < (int)(128 * 1024 * .70); i++) assertTrue(Integer.toBinaryString(i), cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
		for(int i = 0; i < (int)(128 * 1024 * .70); i += 2) assertFalse(Integer.toBinaryString(i), cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
		for(int i = (int)(128 * 1024 * .70); i < 2 * (int)(128 * 1024 * .70); i++) assertTrue(Integer.toBinaryString(i), cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
		for(int i = 0; i < (int)(128 * 1024 * .70); i += 2) assertTrue(Integer.toBinaryString(i), cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
	}

	@Test
	public void testOff() {
		FastApproximateByteArrayCache cache = new FastApproximateByteArrayCache(0);
		for(int i = 0; i < (int)(128 * 1024 * .70); i++) assertTrue(Integer.toBinaryString(i), cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
	}

	@Test
	public void testSmall() {
		FastApproximateByteArrayCache cache = new FastApproximateByteArrayCache((4 * Long.SIZE / Byte.SIZE) * Runtime.getRuntime().availableProcessors());
		for(int i = 0; i < (int)(128 * 1024 * .70); i++) assertTrue(Integer.toBinaryString(i), cache.add(Integer.toBinaryString(i).getBytes(Charsets.ISO_8859_1)));
	}

}
