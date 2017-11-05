package it.unimi.di.law.warc.util;

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

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

public class ReorderingBlockingQueueTest {
	@Test
	public void testNoBlocking() throws InterruptedException {
		for(final int size: new int[] { 1, 10, 100, 128, 256 }) {
			final ReorderingBlockingQueue<Integer> q = new ReorderingBlockingQueue<>(size);
			final int[] perm = Util.identity(size);
			IntArrays.shuffle(perm, new XoRoShiRo128PlusRandom());
			for(int i = perm.length; i-- != 0;) q.put(Integer.valueOf(perm[i]), perm[i]);
			for(int i = 0; i < perm.length; i++) assertEquals(i, q.take().intValue());
			assertEquals(0, q.size());
		}
	}

	@Test
	public void testBlocking() throws InterruptedException {
		for(final int size: new int[] { 10, 100, 128, 256 }) {
			for(final int d: new int[] { 1, 2, 3, 4 }) {
				final ReorderingBlockingQueue<Integer> q = new ReorderingBlockingQueue<>(size / d);
				final int[] perm = Util.identity(size);
				IntArrays.shuffle(perm, new XoRoShiRo128PlusRandom());
				for(int i = perm.length; i-- != 0;) {
					final int t = perm[i];
					new Thread() {
						@Override
						public void run() {
							try {
								q.put(Integer.valueOf(t), t);
							}
							catch (final InterruptedException e) {
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
