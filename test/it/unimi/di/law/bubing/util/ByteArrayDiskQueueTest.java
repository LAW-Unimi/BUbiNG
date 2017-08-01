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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

import it.unimi.dsi.util.XorShift1024StarRandom;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


public class ByteArrayDiskQueueTest {

	@Before
	public void excludeTravis() {
        assumeFalse(System.getenv("TRAVIS") != null); // skip all tests if running under Travis Continuous Integration
	}

	@Test
	public void testSingleEnqueueDequeue() throws IOException {
		final File queue = File.createTempFile(this.getClass().getName(), ".queue");
		queue.deleteOnExit();
		final ByteArrayDiskQueue q = ByteArrayDiskQueue.createNew(queue, 128, true);
		q.enqueue(new byte[1]);
		q.dequeue();
		assertEquals(1, q.buffer().size());
		assertArrayEquals(new byte[1], q.buffer().toByteArray());
		q.close();
	}

	@Test
	public void testSomeEnqueueDequeue() throws IOException {
		File queue = File.createTempFile(this.getClass().getName(), ".queue");
		queue.deleteOnExit();
		ByteArrayDiskQueue q = ByteArrayDiskQueue.createNew(queue, 128, true);
		for(int s = 1; s < 1 << 12; s *= 2) {
			System.err.println("Testing size " + s + "...");
			q.clear();
			for(int i = 0; i < s; i++) q.enqueue(new byte[] { (byte)(i + 64) });
			for(int i = 0; i < s; i++) {
				q.dequeue();
				assertEquals(1, q.buffer().size());
				assertArrayEquals(new byte[] { (byte)(i + 64) }, q.buffer().toByteArray());
			}
		}
		q.close();
	}

	@SuppressWarnings("resource")
	@Test
	public void testQueue() throws IOException {
		final File queue = File.createTempFile(this.getClass().getName(), ".queue");
		queue.deleteOnExit();
		int bufferSize = 128;
		ByteArrayDiskQueue q = ByteArrayDiskQueue.createNew(queue, bufferSize , true);
		final List<byte[]> l = new LinkedList<>();
		final XorShift1024StarRandom random = new XorShift1024StarRandom(0);
		final long start = System.currentTimeMillis();
		double threshold = .8;

		while(System.currentTimeMillis() - start < 20000) {
			assertEquals(l.size(), q.size64());
			switch(random.nextInt(6)) {
			case 0:
				// Invert queue tendency
				if (random.nextFloat() < .01) threshold = 1 - threshold;
				break;
			case 1:
				if (random.nextFloat() < threshold) {
					final URI url = BURL.parse("http://example.com/" + StringUtils.repeat("/" + String.valueOf(random.nextLong()), 1));
					q.enqueue(Util.toByteArray(url.getRawPath()));
					l.add(Util.toByteArray(url.getRawPath()));
				}
				else {
					assertEquals(Boolean.valueOf(q.isEmpty()), Boolean.valueOf(l.isEmpty()));
					if (! q.isEmpty()) {
						final byte[] removed = l.remove(0);
						q.dequeue();
						assertEquals(removed.length, q.buffer().size());
						assertArrayEquals(removed, q.buffer().toByteArray());
					}
				}
				break;
			case 2:
				if (random.nextFloat() < .001) {
					q.clear();
					l.clear();
				}
				/*else if (random.nextFloat() < .01) {
					BinIO.storeObject(q, "testdiskqueue.serialised");
					q.close();
					q = (ByteArrayDiskQueue) BinIO.loadObject("testdiskqueue.serialised");
				}*/
				else if (random.nextFloat() < .01) {
					q.suspend();
				}
				else if (random.nextFloat() < .01) {
					q.trim();
				}
				break;
			case 3:
				q.suspend();
				break;
			case 4:
				if (random.nextFloat() < 0.2) q.enlargeBuffer(bufferSize = Math.min(bufferSize + random.nextInt(bufferSize), 1024 * 1024));
				break;
			case 5:
				if (random.nextFloat() < 0.01) {
					final long size = q.size64();
					q.freeze();
					q = ByteArrayDiskQueue.createFromFile(size, queue, bufferSize, true);
				}
				break;
			}
		}

		q.close();
	}

	@Ignore
	@Test
	public void testLarge() throws IOException {
		final File queue = File.createTempFile(this.getClass().getName(), ".queue");
		queue.deleteOnExit();
		final ByteArrayDiskQueue q = ByteArrayDiskQueue.createNew(queue, 128, true);
		final XorShift1024StarRandom random = new XorShift1024StarRandom(0);

		final long n = 3000000005L;
		for(long i = n; i -- != 0;) q.enqueue(new byte[random.nextInt(4)]);
		assertEquals(n, q.size64());

		final XorShift1024StarRandom random2 = new XorShift1024StarRandom(0);
		for(long i = n; i -- != 0;) {
			q.dequeue();
			assertEquals(random2.nextInt(4), q.buffer().size());
		}

		q.close();
	}

}
