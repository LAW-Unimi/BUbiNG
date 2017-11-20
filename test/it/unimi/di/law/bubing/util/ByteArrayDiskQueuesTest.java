package it.unimi.di.law.bubing.util;

/*
 * Copyright (C) 2012-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

public class ByteArrayDiskQueuesTest {

	@Before
	public void excludeTravis() {
        assumeFalse(System.getenv("TRAVIS") != null); // skip all tests if running under Travis Continuous Integration
	}

	public static final int LOG2_LOG_FILE_SIZE = 18;
	@Test
	public void testReadWriteByte() throws IOException {
		final File dir = File.createTempFile(ByteArrayDiskQueuesTest.class.getName() + "-", "-temp");
		dir.delete();
		dir.mkdir();
		final ByteArrayDiskQueues queues = new ByteArrayDiskQueues(dir, LOG2_LOG_FILE_SIZE);

		for(int pass = 2; pass-- != 0;) {
			queues.pointer(0);
			final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(1);
			for(int i = 0; i < 10000000; i++) queues.write((byte)random.nextInt(256));
			queues.pointer(0);
			random.setSeed(1);
			for(int i = 0; i < 10000000; i++) assertEquals(random.nextInt(256), queues.read());

			for(int i = 10; i-- != 0;) {
				long pointer = random.nextLong(10000000);
				queues.pointer(pointer);
				assertEquals(pointer, queues.pointer());
				random.setSeed(1);
				while(pointer-- != 0) random.nextInt();
				assertEquals(random.nextInt(256), queues.read());
			}
		}

		queues.close();
		FileUtils.deleteDirectory(dir);
	}

	@Test
	public void testReadWriteLong() throws IOException {
		final File dir = File.createTempFile(ByteArrayDiskQueuesTest.class.getName() + "-", "-temp");
		dir.delete();
		dir.mkdir();
		final ByteArrayDiskQueues queues = new ByteArrayDiskQueues(dir, LOG2_LOG_FILE_SIZE);

		for(int start = 0; start < 8; start++) {
			queues.pointer(start);
			final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(1);
			for(int i = 0; i < 100000; i++) queues.writeLong(random.nextLong());
			queues.pointer(start);
			random.setSeed(1);
			for(int i = 0; i < 100000; i++) assertEquals(random.nextLong(), queues.readLong());

			for(int i = 10; i-- != 0;) {
				long pointer = random.nextLong(100000);
				queues.pointer(start + 8L * pointer);
				assertEquals(start + 8L * pointer, queues.pointer());
				random.setSeed(1);
				while(pointer-- != 0) random.nextLong();
				assertEquals(random.nextLong(), queues.readLong());
			}
		}

		queues.close();
		FileUtils.deleteDirectory(dir);
	}

	@Test
	public void testReadWriteByteArray() throws IOException {
		final File dir = File.createTempFile(ByteArrayDiskQueuesTest.class.getName() + "-", "-temp");
		dir.delete();
		dir.mkdir();
		final ByteArrayDiskQueues queues = new ByteArrayDiskQueues(dir, LOG2_LOG_FILE_SIZE);

		final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(1);
		queues.pointer(0);
		for(int i = 0; i < 100000; i++) {
			final int length = random.nextInt(200);
			final byte[] array = new byte[length];
			for(int p = array.length; p-- != 0;) array[p] = (byte)p;
			queues.write(array, 0, length);
		}
		queues.pointer(0);
		random.setSeed(1);
		for(int i = 0; i < 100000; i++) {
			final int length = random.nextInt(200);
			final byte[] array = new byte[length];
			queues.read(array, 0, length);
			for(int p = length; p-- != 0;) assertEquals((byte)p, array[p]);
		}

		for(int i = 10; i-- != 0;) {
			int a = random.nextInt(100000);
			long pointer = 0;
			random.setSeed(1);
			while(a-- != 0) pointer += random.nextInt(200);
			queues.pointer(pointer);
			assertEquals(pointer, queues.pointer());
			final int length = random.nextInt(200);
			final byte[] array = new byte[length];
			queues.read(array, 0, length);
			for(int p = length; p-- != 0;) assertEquals((byte)p, array[p]);
		}

		queues.close();
		FileUtils.deleteDirectory(dir);
	}

	@Test
	public void testReadWriteVByte() throws IOException {
		final File dir = File.createTempFile(ByteArrayDiskQueuesTest.class.getName() + "-", "-temp");
		dir.delete();
		dir.mkdir();
		final ByteArrayDiskQueues queues = new ByteArrayDiskQueues(dir, LOG2_LOG_FILE_SIZE);

		final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(1);
		queues.pointer(0);
		for(int i = 0; i < 10000000; i++) queues.encodeInt(random.nextInt(Integer.MAX_VALUE));
		queues.pointer(0);
		random.setSeed(1);
		for(int i = 0; i < 10000000; i++) assertEquals(random.nextInt(Integer.MAX_VALUE), queues.decodeInt());

		queues.close();
		FileUtils.deleteDirectory(dir);
	}

	@Test
	public void test() throws IOException {
		final File dir = File.createTempFile(ByteArrayDiskQueuesTest.class.getName() + "-", "-temp");
		dir.delete();
		dir.mkdir();
		final ByteArrayDiskQueues queues = new ByteArrayDiskQueues(dir, LOG2_LOG_FILE_SIZE);

		final Object[] key = new Object[10000];
		final IntArrayFIFOQueue[] lengths = new IntArrayFIFOQueue[key.length];
		for(int i = key.length; i-- != 0;) {
			key[i] = Integer.valueOf(i);
			lengths[i] = new IntArrayFIFOQueue();
		}
		long size = 0;
		final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(1);
		queues.collect(1); // To increase coverage
		for(int i = 0; i < 10000000; i++) {
			final int keyIndex = random.nextInt(key.length);
			final int length = random.nextInt(200);
			lengths[keyIndex].enqueue(length);
			final byte[] array = new byte[length];
			for(int p = array.length; p-- != 0;) array[p] = (byte)p;
			queues.enqueue(key[keyIndex], array, 0, array.length);
			size++;

			if (random.nextInt(100) == 0) {
				final int howMany = random.nextInt(lengths[keyIndex].size());
				for(int j = 0; j < howMany; j++) {
					final byte[] d = queues.dequeue(key[keyIndex]);
					size--;
					assertEquals(lengths[keyIndex].dequeueInt(), d.length);
					for(int p = d.length; p-- != 0;) assertEquals((byte)p, d[p]);
				}

				for(int j = 0; j < key.length; j++) assertEquals(lengths[j].size(), queues.count(key[j]));

			}

			if (random.nextInt(1000) == 0) {
				final int k = random.nextInt(key.length);
				queues.remove(key[k]);
				size -= lengths[k].size();
				lengths[k].clear();
				assertEquals(0, queues.count(key[k]));
			}

			if (random.nextInt(100000) == 0) queues.collect(random.nextInt(10) == 0 ? 1 : .75);
			assertEquals(size, queues.size64());
		}

		for(int i = 0; i < key.length; i++) {
			assertEquals(lengths[i].size(), queues.count(key[i]));
			while (! lengths[i].isEmpty()) {
				final byte[] d = queues.dequeue(key[i]);
				size--;
				assertEquals(lengths[i].dequeueInt(), d.length);
				for(int p = d.length; p-- != 0;) assertEquals((byte)p, d[p]);
			}
		}


		queues.close();
		FileUtils.deleteDirectory(dir);
	}
}
