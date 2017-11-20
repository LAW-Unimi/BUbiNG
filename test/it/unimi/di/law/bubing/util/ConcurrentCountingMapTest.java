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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

public class ConcurrentCountingMapTest {
	@Test
	public void test() {
		final ConcurrentCountingMap map = new ConcurrentCountingMap(4);
		assertEquals(0, map.addTo(new byte[1], 1));
		assertEquals(1, map.addTo(new byte[1], 1));
		assertEquals(2, map.get(new byte[] { 1, 0, 1 }, 1, 1));
		assertEquals(0, map.addTo(new byte[0], 3));
		assertEquals(3, map.put(new byte[0], 10));
	}

	@Test
	public void testLarge() throws IOException, ClassNotFoundException {
		ConcurrentCountingMap map = new ConcurrentCountingMap(4);
		final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(0);
		final Object2IntOpenCustomHashMap<byte[]> hashMap = new Object2IntOpenCustomHashMap<>(ByteArrays.HASH_STRATEGY);
		for(int i = 0; i < 1000000; i++) {
			final int length = random.nextInt(100);
			final int offset = random.nextInt(3);
			final int padding = random.nextInt(3);
			final byte[] key = new byte[offset + length + padding];
			for(int p = key.length; p-- != 0;) key[p] = (byte)random.nextInt(4);
			final byte[] exactKey = Arrays.copyOfRange(key, offset, offset + length);
			switch(random.nextInt(3)) {
			case 0:
				final int delta = random.nextInt(3) + 1;
				assertEquals(hashMap.addTo(exactKey, delta), map.addTo(key, offset, length, delta));
				break;
			case 1:
				final int value = random.nextInt(3) + 1;
				assertEquals(hashMap.put(exactKey, value), map.put(key, offset, length, value));
				break;
			case 2:
				assertEquals(hashMap.getInt(exactKey), map.get(key, offset, length));
			}
		}

		for(final ObjectIterator<Entry<byte[]>> iterator = hashMap.object2IntEntrySet().fastIterator(); iterator.hasNext();) {
			final Entry<byte[]> next = iterator.next();
			assertEquals(Arrays.toString(next.getKey()), next.getIntValue(), map.get(next.getKey()));
		}

		final File temp = File.createTempFile(ConcurrentCountingMap.class.getSimpleName() + "-", "-temp");
		temp.deleteOnExit();
		BinIO.storeObject(map, temp);
		map = (ConcurrentCountingMap)BinIO.loadObject(temp);
		for(final ObjectIterator<Entry<byte[]>> iterator = hashMap.object2IntEntrySet().fastIterator(); iterator.hasNext();) {
			final Entry<byte[]> next = iterator.next();
			assertEquals(Arrays.toString(next.getKey()), next.getIntValue(), map.get(next.getKey()));
		}

		temp.delete();
	}

}
