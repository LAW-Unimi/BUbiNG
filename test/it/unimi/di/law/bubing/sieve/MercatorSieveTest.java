package it.unimi.di.law.bubing.sieve;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.lang.MutableString;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assume.assumeFalse;

import com.google.common.io.Files;


public class MercatorSieveTest {

	@Before
	public void excludeTravis() {
        assumeFalse(System.getenv("TRAVIS") != null); // skip all tests if running under Travis Continuous Integration
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testMultiThreadedTestSequentialDequeue() throws InterruptedException, IOException {
		final Random random = new Random(0);
		final int NUM_PAIRS = 100 + random.nextInt(100); // Number of key/value pairs that may be inserted
		final int NUM_THREADS = 5 + random.nextInt(20); // Number of insertion threads
		final AbstractSieve.DiskNewFlow<CharSequence> newFlow = new AbstractSieve.DiskNewFlow<>(CharSequenceByteSerializerDeserializer.getInstance());
		final File tempDir = Files.createTempDir();
		System.err.println("testMultiThreadedTestSequentialDequeue() store in " + tempDir);
		final AbstractSieve<CharSequence,Integer> sieve = new MercatorSieve<>(true, tempDir, NUM_THREADS * 100 * NUM_PAIRS, 512, NUM_THREADS * 100 * NUM_PAIRS, newFlow, CharSequenceByteSerializerDeserializer.getInstance(), ByteSerializerDeserializer.INTEGER, MercatorSieve.CHAR_SEQUENCE_HASHING_STRATEGY, null);

		// Creates NUM_PAIRS pairs of the form (Axxx,yyy) to be inserted
		final AbstractSieve.SieveEntry<CharSequence,Integer> pairsToBeInserted[] = new AbstractSieve.SieveEntry[NUM_PAIRS];
		final boolean insertedAlready[] = new boolean[NUM_PAIRS];
		for (int i = 0; i < NUM_PAIRS; i++) {
			String key = "A" + i;
			Integer value = new Integer(random.nextInt(1000));
			pairsToBeInserted[i] = new AbstractSieve.SieveEntry<>(key, value);
			insertedAlready[i] = false;
		}
		// An array of threads: each of them waits a random number of milliseconds (between 0 and 40), selects one of the entries to be inserted (if it was already inserted, the pair is inserted with a value increased by 1)
		// This is repeated a random number of times, between 10 and 100
		Thread insertionThread[] = new Thread[NUM_THREADS];
		for (int i = 0; i < NUM_THREADS; i++)
			insertionThread[i] = new Thread() {
				Random random1 = new Random(0);
				@Override
				public void run() {
					try {
						int times = 10 + random1.nextInt(90);
						for (int i = 0; i < times; i++) {
							sleep(random1.nextInt(40));
							synchronized (pairsToBeInserted) {
								int pos = random1.nextInt(NUM_PAIRS);
								boolean wasThere = insertedAlready[pos];
								insertedAlready[pos] = true;
								sieve.enqueue(pairsToBeInserted[pos].key, new Integer(pairsToBeInserted[pos].value.intValue() + (wasThere? 1 : 0)));
							}
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			};
		// Perform all insertions
		for (Thread thread: insertionThread) thread.start();
		for (Thread thread: insertionThread) thread.join();
		// Now close and dequeue everything
		sieve.close();
		newFlow.noMoreAppend();
		for (;;) {
			try {
				MutableString dequeued = newFlow.dequeueKey();
				int pos = Integer.parseInt(new StringBuilder(dequeued.subSequence(1, dequeued.length())).toString());
				assertEquals("A" + pos, dequeued.toString());
				assertEquals(pairsToBeInserted[pos].key, dequeued.toString());
				assertTrue(insertedAlready[pos]);
				//assertEquals(pairsToBeInserted[pos].value, dequeued.value);
				insertedAlready[pos] = false;
			} catch (NoSuchElementException e) {
				break;
			}
		}
		for (boolean insAl: insertedAlready) assertFalse(insAl);

		FileUtils.deleteDirectory(tempDir);
	}


	@Test
	public void testMultiThreadedTestParallelDequeue() throws InterruptedException, IOException {
		final Random random = new Random(0);

		final AbstractSieve.DiskNewFlow<CharSequence> newFlow = new AbstractSieve.DiskNewFlow<>(CharSequenceByteSerializerDeserializer.getInstance());
		final File tempDir = Files.createTempDir();
		System.err.println("testMultiThreadedTestParallelDequeue() store in " + tempDir);
		final AbstractSieve<CharSequence,Integer> sieve = new MercatorSieve<>(true, tempDir, 1024, 512, 1024, newFlow, CharSequenceByteSerializerDeserializer.getInstance(), ByteSerializerDeserializer.INTEGER, MercatorSieve.CHAR_SEQUENCE_HASHING_STRATEGY, null);

		// Insertion
		final int NUM_PAIRS = 10000 + random.nextInt(100000); // Number of key/value pairs that every thread inserts
		final int NUM_ENQUEUE_THREADS = 5 + random.nextInt(100); // Number of insertion threads
		final double PROB_REINSERTION = 0.1; // Probability that a thread decides to re-insert a pair that is already present
		final ObjectOpenHashSet<MutableString> enqueued = new ObjectOpenHashSet<>(); // Elements that were inserted

		final Thread[] enqueueThread = new Thread[NUM_ENQUEUE_THREADS];
		for (int i = 0; i < NUM_ENQUEUE_THREADS; i++)
			enqueueThread[i] = new Thread() {
				Random r = new Random(0);
				@Override
				public void run() {
					MutableString toBeInserted = null;
					for (int times = 0; times < NUM_PAIRS; times++) {
						// Choose an element from the set of already inserted
						if (r.nextDouble() < PROB_REINSERTION && enqueued.size() > 0) {
							int t = r.nextInt(enqueued.size()), j = 0;
							synchronized(enqueued) {
								for (MutableString element: enqueued)
									if (j++ == t) {
										toBeInserted = element;
										break;
									}
							}
							//System.err.println(times + "/" + NUM_PAIRS + " --- Requeuing " + toBeInserted + " by " + Thread.currentThread());
						}
						// Or draw it at random
						else {
							toBeInserted = new MutableString("A" + r.nextInt(1000000));
							//System.err.println(times + "/" + NUM_PAIRS + " --- Enqueuing " + toBeInserted + " by " + Thread.currentThread());
							synchronized(enqueued) {
								enqueued.add(toBeInserted);
							}
						}
						try {
							//System.err.println(times + "/" + NUM_PAIRS + " --- Enqueuing " + toBeInserted + " by " + Thread.currentThread());
							sieve.enqueue(toBeInserted, null);
						}
						catch (IOException e) {
							e.printStackTrace();
						}
						catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			};

		// Flushing thread
		final int MIN_FLUSH = 100; // Minimum number of milliseconds between flushes
		final int MAX_FLUSH = 4000; // Maximum number of milliseconds between flushes
		final Thread flushingThread = new Thread() {
			@Override
			public void run() {
				Random r = new Random(0);
				try {
					for (;;) {
						sleep(MIN_FLUSH + r.nextInt(MAX_FLUSH - MIN_FLUSH));
						//System.err.println("Going to force flush");
						sieve.flush();
						if (Thread.currentThread().isInterrupted()) return;
					}
				}
				catch (InterruptedException e) {
					return;
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		};

		// Dequeuing threads
		final ObjectOpenHashSet<MutableString> dequeued = new ObjectOpenHashSet<>(); // Elements that were inserted
		final int NUM_DEQUEUE_THREADS = 5 + random.nextInt(100); // Number of extraction threads

		final Thread[] dequeueThread = new Thread[NUM_DEQUEUE_THREADS];
		for (int i = 0; i < NUM_DEQUEUE_THREADS; i++)
			dequeueThread[i] = new Thread() {
				@Override
				public void run() {
					try {
						for (;;) {
							MutableString key = newFlow.dequeueKey();
							//System.err.println("Dequeued " + key + " by " + Thread.currentThread());
							synchronized (dequeued) {
								//System.err.println("\tCurrently dequeued contains " + dequeued);
								assertFalse("Key " + key + " has been decoded twice", dequeued.contains(key)); // Cannot dequeue the same key twice
								dequeued.add(key.copy());
							}
						}
					}
					catch (NoSuchElementException e) {
						// Standard exit
					}
					catch (IOException e) {
						e.printStackTrace();
					}
					catch (InterruptedException e) {
					}
				}
		};

		// Job that monitors the status of dequeuing threads
		final Thread monitorThread = new Thread() {
			@Override
			@SuppressWarnings("boxing")
			public void run() {
				for (;;) {
					int countInterrupted = 0, countAlive = 0, countWaiting = 0, countBlocked = 0;
					List<State> stateList = new ArrayList<>();
					for (Thread thread: dequeueThread) {
						if (thread.isInterrupted()) countInterrupted++;
						if (thread.isAlive()) countAlive++;
						if (thread.getState() == State.WAITING) countWaiting++;
						if (thread.getState() == State.BLOCKED) countBlocked++;
						stateList.add(thread.getState());
					}
					System.err.printf("Monitor:\ttotal=%d, alive=%d, interrupted=%d, waiting=%d, blocked=%d\n", dequeueThread.length, countAlive, countInterrupted, countWaiting, countBlocked);
					System.err.println("\t\t" + stateList);
					try {
						sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						return;
					}
				}
			}
		};

		// Start the monitoring thread
		monitorThread.start();

		// Start insertion threads
		for (Thread thread: enqueueThread) thread.start();
		// Start flushing thread
		flushingThread.start();
		// Start dequeuing threads
		for (Thread thread: dequeueThread) thread.start();

		// Wait insertion threads to end
		System.err.println("Going to interrupt flush");
		// Interrupt flushing thread
		flushingThread.interrupt();
		System.err.println("Flush interrupted");

		System.err.println("Waiting for enqueuing threads to end");
		for (Thread thread: enqueueThread) thread.join();

		// Close
		sieve.close();
		newFlow.noMoreAppend();
		// Wait dequeuing threads to end
		System.err.println("Waiting for dequeuing threads to end");
		for (Thread thread: dequeueThread) thread.interrupt();
		for (Thread thread: dequeueThread) thread.join();
		System.err.println("Enqueued: " + enqueued.size() + " Dequeued: " + dequeued.size());
		final ObjectOpenHashSet<MutableString> enqueuedClone = enqueued.clone();
		enqueuedClone.removeAll(dequeued);
		System.err.println("Enqueued but not dequeued: (" + enqueuedClone.size() + "): " + enqueuedClone);
		final ObjectOpenHashSet<MutableString> dequeuedClone = dequeued.clone();
		dequeuedClone.removeAll(enqueued);
		System.err.println("Dequeued but not enqueued: (" + dequeuedClone.size() + "): " + dequeuedClone);

		assertEquals(enqueued, dequeued);
		FileUtils.deleteDirectory(tempDir);
	}

	@Test
	public void testSimple() throws IOException, InterruptedException {
		final AbstractSieve.DiskNewFlow<CharSequence> newFlow = new AbstractSieve.DiskNewFlow<>(CharSequenceByteSerializerDeserializer.getInstance());
		final File tempDir = Files.createTempDir();
		System.err.println("testSimple() store in " + tempDir);
		AbstractSieve<CharSequence,Integer> sieve = new MercatorSieve<>(true, tempDir, 1024, 16, 1024, newFlow, CharSequenceByteSerializerDeserializer.getInstance(), ByteSerializerDeserializer.INTEGER, MercatorSieve.CHAR_SEQUENCE_HASHING_STRATEGY, null);

		sieve.enqueue("A0", new Integer(0));
		sieve.enqueue("A1", new Integer(1));
		sieve.enqueue("A0", new Integer(3)); // Will not be dequeued
		sieve.enqueue("A3", new Integer(2));
		Thread.sleep(2000); // Give time to the dequeueing thread.
		sieve.flush();

		CharSequence result;
		result = newFlow.dequeueKey(); // Get A0
		assertEquals("A0", result.toString());
		result = newFlow.dequeueKey(); // Get A1
		assertEquals("A1", result.toString());
		result = newFlow.dequeueKey(); // Get A3
		assertEquals("A3", result.toString());

		sieve.enqueue("A0", new Integer(1));
		sieve.enqueue("A5", new Integer(1));
		sieve.enqueue("A2", new Integer(0));
		sieve.enqueue("A2", new Integer(3)); // Will not be dequeued
		sieve.enqueue("A5", new Integer(1));
		sieve.enqueue("A6", new Integer(2));
		Thread.sleep(2000); // Give time to the dequeueing thread.
		sieve.flush();
		sieve.close();
		newFlow.noMoreAppend();

		result = newFlow.dequeueKey();
		assertEquals("A5", result.toString());
		result = newFlow.dequeueKey();
		assertEquals("A2", result.toString());
		result = newFlow.dequeueKey();
		assertEquals("A6", result.toString());
		FileUtils.deleteDirectory(tempDir);
	}
}
