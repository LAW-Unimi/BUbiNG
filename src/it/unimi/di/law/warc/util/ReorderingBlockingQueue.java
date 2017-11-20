package it.unimi.di.law.warc.util;

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

// RELEASE-STATUS: DIST

import it.unimi.dsi.fastutil.HashCommon;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** A blocking queue holding a fixed amount of <em>timestamped</em> items. The {@link #put(Object, long)}
 * must be called with an object and a timestamp. Timestamps must be a contiguous interval of the
 * natural numbers starting at zero, and objects will be returned in timestamp order. Failure to
 * comply with the contract (i.e., missing timestamps) will cause the queue to block forever.
 *
 * <p>{@link #put(Object, long)} might block if there is not enough space to keep track of the
 * object (i.e., if its timestamp is too far in time w.r.t. the timestamp that would be
 * returned next by the queue). {@link #take()} might block if the object with the next
 * timestamp has not been {@link #put(Object, long)} yet.
 *
 * <p>All methods of this class complete in constant time.
 */
public class ReorderingBlockingQueue<E> {
    /** The backing array. */
    private final Object[] a;
    /** The length of {@link #a} minus one, cached. */
    private final int mask;
    /** The current position into {@link #a} (the position of the next object to be returned). */
    private int start;
    /** The timestamp of the next object to be returned. */
    private long timeStamp;
    /** The number of elements in the queue. */
    private int count;
    /** The main lock. */
    private final ReentrantLock lock;
    /** Condition for waiting takes. */
    private final Condition nextObjectReady;
    /** Condition for waiting puts. */
    private final Condition newSpaceAvailable;

    /** Creates a {@code ReorderingBlockingQueue} with the given fixed
     * capacity.
     *
     * @param capacity the capacity of this queue (will be rounded to the next power of two).
     */
	public ReorderingBlockingQueue(final int capacity) {
		if (capacity <= 0) throw new IllegalArgumentException();
		a = new Object[HashCommon.nextPowerOfTwo(capacity)];
		mask = a.length - 1;
		lock = new ReentrantLock(false);
		nextObjectReady = lock.newCondition();
		newSpaceAvailable = lock.newCondition();
	}

    /** Inserts an element with given timestamp, waiting for space to become available
     * if the timestamp of the element minus the current timestamp of the queue exceeds
     * the queue capacity.
     *
     * @param e an element.
     * @param timeStamp the timestamp of {@code e}.
     */
	public void put(final E e, final long timeStamp) throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			// mask is a.length - 1
			while(timeStamp - this.timeStamp > mask) newSpaceAvailable.await();
			final int timeOffset = (int)(timeStamp - this.timeStamp);
			assert a[start + timeOffset & mask] == null : a[start + timeOffset & mask];
			a[start + timeOffset & mask] = e;
			++count;
			if (timeOffset == 0) nextObjectReady.signal();
		}
		finally {
			lock.unlock();
		}
	}

    /** Returns the element with the next timestamp, waiting until it is available.
     *
     * <p>Note that because of the reordering semantics, an invocation of this method
     * on a {@linkplain #isEmpty() nonempty} queue might block nonetheless.
     *
     * @return the element with the next timestamp.
     */
	public E take() throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while (a[start] == null) nextObjectReady.await();
			@SuppressWarnings("unchecked")
			final E x = (E)a[start];
			a[start] = null;
			start = start + 1 & mask;
			--count;
			timeStamp++;
			newSpaceAvailable.signalAll();
			return x;
		}
		finally {
			lock.unlock();
		}
	}

    /** Returns the number of elements in this queue.
     *
     * @return the number of elements in this queue
     * @see #isEmpty()
     */
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    /** Returns whether this queue is empty.
     *
     * @return whether this queue is empty.
     */
    public boolean isEmpty() {
    	return size() == 0;
    }
}
