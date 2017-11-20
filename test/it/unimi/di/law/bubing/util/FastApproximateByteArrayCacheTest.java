package it.unimi.di.law.bubing.util;

/*
 * Copyright (C) 2004-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
