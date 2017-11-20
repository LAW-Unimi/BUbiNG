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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

//RELEASE-STATUS: DIST

/** A thin layer around a {@link ConcurrentLinkedQueue} that exhibits a subset of the available methods,
 * and keeps track in an {@link AtomicLong} of the size of the queue,
 * so that {@link #size()} can return in constant time.
 *
 * @author Sebastiano Vigna
 */

public class LockFreeQueue<T> {
	/** The underlying concurrent queue. */
	private final ConcurrentLinkedQueue<T> queue;
	/** The approximate size of {@link #queue}. */
	private final AtomicLong size;

	public LockFreeQueue() {
		queue = new ConcurrentLinkedQueue<>();
		size = new AtomicLong();
	}

	/**
	 * @see Queue#add(Object)
	 */
	public boolean add(T e) {
		size.incrementAndGet();
		return queue.add(e);
	}

	/**
	 * @see Queue#poll()
	 */
	public T poll() {
		final T result = queue.poll();
		if (result != null) size.decrementAndGet();
		return result;
	}

	/** Returns the (approximate) size of this queue.
	 *
	 * <p>This methods returns in constant time (it is just an {@link AtomicLong#get()}).
	 * Due to concurrent modifications, the returned value might not reflect the actual number of elements in the queue.
	 *
	 * @return the (approximate) size of this queue.
	 */
	public long size() {
		return size.get();
	}

	@Override
	public int hashCode() {
		return queue.hashCode();
	}

	@Override
	public boolean equals(final Object o) {
		return queue.equals(o);
	}
}
