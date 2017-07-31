package it.unimi.di.law.warc.util;

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

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.util.XorShift1024StarRandom;

import org.junit.Test;

public class ReorderingBlockingQueueTest {
	@Test
	public void testNoBlocking() throws InterruptedException {
		for(int size: new int[] { 1, 10, 100, 128, 256 }) {
			final ReorderingBlockingQueue<Integer> q = new ReorderingBlockingQueue<>(size);
			final int[] perm = Util.identity(size);
			IntArrays.shuffle(perm, new XorShift1024StarRandom());
			for(int i = perm.length; i-- != 0;) q.put(Integer.valueOf(perm[i]), perm[i]);
			for(int i = 0; i < perm.length; i++) assertEquals(i, q.take().intValue());
			assertEquals(0, q.size());
		}
	}

	@Test
	public void testBlocking() throws InterruptedException {
		for(int size: new int[] { 10, 100, 128, 256 }) {
			for(int d: new int[] { 1, 2, 3, 4 }) {
				final ReorderingBlockingQueue<Integer> q = new ReorderingBlockingQueue<>(size / d);
				final int[] perm = Util.identity(size);
				IntArrays.shuffle(perm, new XorShift1024StarRandom());
				for(int i = perm.length; i-- != 0;) {
					final int t = perm[i];
					new Thread() {
						@Override
						public void run() {
							try {
								q.put(Integer.valueOf(t), t);
							}
							catch (InterruptedException e) {
								throw new RuntimeException(e.getMessage(), e);
							}
						}
					}.start();
				}
				for(int i = 0; i < perm.length; i++) assertEquals(i, q.take().intValue());
				assertEquals(0, q.size());
			}
		}
	}
}
