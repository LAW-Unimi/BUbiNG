package it.unimi.di.law.bubing.util;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
