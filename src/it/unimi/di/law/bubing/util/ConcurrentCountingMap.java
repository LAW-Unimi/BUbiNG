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

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.Hash;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

//RELEASE-STATUS: DIST

/** A concurrent counting map. The map is made by a number of <em>stripes</em>
 * which are accessed independently
 * using a {@link ReentrantReadWriteLock}. Only one thread can write in a stripe at a time, but different stripes
 * can be modified independently and read access can happen concurrently on each stripe.
 *
 * <p>Keys are sequences of bytes specified as byte-array fragments. Note that when adding a new key (either
 * by means of {@link #put(byte[], int, int, int)} or by means of {@link #addTo(byte[], int, int, int)}),
 * if the specified array fragment is not used as key in the data structure.
 */

public class ConcurrentCountingMap implements Serializable {
	private static final long serialVersionUID = 1L;

	/** The stripes. Keys are distributed among them using the lower bits of their {@link Object#hashCode()}. */
	private final Stripe[] stripe;
	/** An array of locks parallel to {@link #stripe}, protecting each stripe. */
	private final ReentrantReadWriteLock[] lock;
	/** 64 minus the base-2 logarithm of {@link #stripe stripe.length}, cached. */
	private final int shift;

	/** Creates a new concurrent counting map with concurrency level equal to {@link Runtime#availableProcessors()}. */
	public ConcurrentCountingMap() {
		this(Runtime.getRuntime().availableProcessors());
	}

	/** Creates a new concurrent counting map.
	 *
	 * @param concurrencyLevel the number of stripes (it will be {@linkplain Integer#highestOneBit(int) forced to be a power of two}).
	 */
	public ConcurrentCountingMap(final int concurrencyLevel) {
		stripe = new Stripe[Math.max(2, Integer.highestOneBit(concurrencyLevel))];
		lock = new ReentrantReadWriteLock[stripe.length];
		for(int i = stripe.length; i-- != 0;) {
			stripe[i] = new Stripe();
			lock[i] = new ReentrantReadWriteLock();
		}
		shift = 64 - Fast.mostSignificantBit(stripe.length);
	}

	/** Gets the value of the counter associated with a given key.
	 *
	 * @param array a byte array.
	 * @return the current value of the counter associated with the specified key.
	 */
	public int get(final byte[] array) {
		return get(array, 0, array.length);
	}

	/** Gets the value of the counter associated with a given key.
	 *
	 * @param array a byte array.
	 * @param offset the first valid byte in {@code array}.
	 * @param length the number of valid elements in {@code array}.
	 * @return the current value of the counter associated with the specified key.
	 */
	public int get(final byte[] array, final int offset, final int length) {
		final long hash = MurmurHash3.hash(array, offset, length);
		final ReadLock readLock = lock[(int)(hash >>> shift)].readLock();
		try {
			readLock.lock();
			return stripe[(int)(hash >>> shift)].get(array, offset, length, hash);
		}
		finally {
			readLock.unlock();
		}
	}

	/** Adds a value to the counter associated with a given key.
	 *
	 * @param array a byte array.
	 * @param delta a value to be added to the counter associated with the specified key.
	 * @return the previous value of the counter associated with the specified key.
	 */
	public int addTo(final byte[] array, final int delta) {
		return addTo(array, 0, array.length, delta);
	}

	/** Adds a value to the counter associated with a given key.
	 *
	 * @param array a byte array.
	 * @param offset the first valid byte in {@code array}.
	 * @param length the number of valid elements in {@code array}.
	 * @param delta a value to be added to the counter associated with the specified key.
	 * @return the previous value of the counter associated with the specified key.
	 */
	public int addTo(final byte[] array, final int offset, final int length, final int delta) {
		final long hash = MurmurHash3.hash(array, offset, length);
		final WriteLock writeLock = lock[(int)(hash >>> shift)].writeLock();
		try {
			writeLock.lock();
			return stripe[(int)(hash >>> shift)].addTo(array, offset, length, hash, delta);
		}
		finally {
			writeLock.unlock();
		}
	}

	/** Sets the value associated with a given key.
	 *
	 * @param array a byte array.
	 * @param value a value to be associated with the specified key.
	 * @return the previous value of the counter associated with the specified key.
	 */
	public int put(final byte[] array, final int value) {
		return put(array, 0, array.length, value);
	}

	/** Sets the value associated with a given key.
	 *
	 * @param array a byte array.
	 * @param offset the first valid byte in {@code array}.
	 * @param length the number of valid elements in {@code array}.
	 * @param value a value to be associated with the specified key.
	 * @return the previous value of the counter associated with the specified key.
	 */
	public int put(final byte[] array, final int offset, final int length, final int value) {
		final long hash = MurmurHash3.hash(array, offset, length);
		final WriteLock writeLock = lock[(int)(hash >>> shift)].writeLock();
		try {
			writeLock.lock();
			return stripe[(int)(hash >>> shift)].put(array, offset, length, hash, value);
		}
		finally {
			writeLock.unlock();
		}
	}

	/** Acquires a locked copy of this map.
	 *
	 * <p>The locked copy has the same method of a {@link ConcurrentCountingMap}, but without
	 * concurrency overhead. It is useful for bulk operations.
	 * All write locks are acquired when this method is called, and
	 * <strong>must</strong> be {@linkplain LockedMap#unlock() released} after the bulk operation has completed.
	 * A typical usage is
	 *
	 * <pre>
	 *	LockedMap lockedMap;
	 *	try {
	 *		lockedMap = concurrentCountingMap.lock();
	 *		// bulk operations
	 *	}
	 *	finally {
	 *		lockedMap.unlock();
	 *	}
	 * </pre>
	 *
	 * @return a locked copy of this map.
	 */
	public LockedMap lock() {
		return new LockedMap(this);
	}

	public static final class LockedMap {
		private boolean released;
		private final Stripe[] stripe;
		private final WriteLock[] writeLock;
		private final int shift;

		public LockedMap(ConcurrentCountingMap concurrentCountingMap) {
			stripe = concurrentCountingMap.stripe;
			writeLock = new WriteLock[concurrentCountingMap.stripe.length];
			shift = concurrentCountingMap.shift;
			for(int i = writeLock.length; i-- != 0;) (writeLock[i] = concurrentCountingMap.lock[i].writeLock()).lock();
		}

		public void unlock() {
			for(WriteLock w: writeLock) w.unlock();
			released = true;
		}

		/** @see ConcurrentCountingMap#get(byte[]) */
		public int get(final byte[] array) {
			return get(array, 0, array.length);
		}

		/** @see ConcurrentCountingMap#get(byte[], int, int) */
		public int get(final byte[] array, final int offset, final int length) {
			if (released) throw new IllegalStateException();
			final long hash = MurmurHash3.hash(array, offset, length);
			return stripe[(int)(hash >>> shift)].get(array, offset, length, hash);
		}

		/** @see ConcurrentCountingMap#addTo(byte[], int) */
		public int addTo(final byte[] array, final int delta) {
			return addTo(array, 0, array.length, delta);
		}

		/** @see ConcurrentCountingMap#addTo(byte[], int, int, int) */
		public int addTo(final byte[] array, final int offset, final int length, final int delta) {
			if (released) throw new IllegalStateException();
			final long hash = MurmurHash3.hash(array, offset, length);
			return stripe[(int)(hash >>> shift)].addTo(array, offset, length, hash, delta);
		}

		/** @see ConcurrentCountingMap#put(byte[], int) */
		public int put(final byte[] array, final int value) {
			return put(array, 0, array.length, value);
		}

		/** @see ConcurrentCountingMap#put(byte[], int, int, int) */
		public int put(final byte[] array, final int offset, final int length, final int value) {
			if (released) throw new IllegalStateException();
			final long hash = MurmurHash3.hash(array, offset, length);
			return stripe[(int)(hash >>> shift)].put(array, offset, length, hash, value);
		}
	}


	protected static final class Stripe implements java.io.Serializable, Cloneable, Hash {
		private static final long serialVersionUID = 0L;
		/** The array of keys. */
		protected transient byte[][] key;
		/** The array of values. */
		protected transient int[] value;
		/** The current table size. */
		protected int n;
		/** Threshold after which we rehash. */
		protected int maxFill;
		/** The mask for wrapping a position counter. */
		protected transient int mask;
		/** Number of entries in the stripe. */
		protected int size;

		/** Creates a new stripe.*/
		protected Stripe() {
			n = 1024;
			mask = n - 1;
			maxFill = 3 * (n / 4);
			key = new byte[n][];
			value = new int[n];
		}

		/** Checks that the content of an array is equal to the content of an array fragment, assuming they have the same length (otherwise, behavior will be impredictable).
		 *
		 * @param a an array.
		 * @param b another array.
		 * @param offset the first valid byte in {@code b}.
		 * @param length the number of valid elements in {@code b}.
		 * @return true of the content of {@code a} is equal to the content of the specified fragment of {@code b}.
		 */
		private static final boolean equals(final byte[] a, final byte[] b, final int offset, int length) {
			while(length-- != 0) if (a[length] != b[offset + length]) return false;
			return true;
		}

		public int put(final byte[] array, final int offset, final int length, long hash, final int v) {
			// The starting point.
			int pos = (int)(hash & mask);
			// There's always an unused entry.
			while (key[pos] != null) {
				if (key[pos].length == length && equals(key[pos], array, offset, length)) {
					final int oldValue = value[pos];
					value[pos] = v;
					return oldValue;
				}
				pos = (pos + 1) & mask;
			}
			// Exact keys are kept
			key[pos] = Arrays.copyOfRange(array, offset, offset + length);
			value[pos] = v;
			if (++size >= maxFill) rehash(n * 2);
			return 0;
		}

		public int addTo(final byte[] array, final int offset, final int length, final long hash, final int incr) {
			// The starting point.
			int pos = (int)(hash & mask);
			// There's always an unused entry.
			while (key[pos] != null) {
				if (key[pos].length == length && equals(key[pos], array, offset, length)) {
					final int oldValue = value[pos];
					value[pos] += incr;
					return oldValue;
				}
				pos = (pos + 1) & mask;
			}
			key[pos] = Arrays.copyOfRange(array,  offset, offset + length);
			value[pos] = incr;
			if (++size >= maxFill) rehash(n * 2);
			return 0;
		}

		/** Shifts left entries with the specified hash code, starting at the specified position, and
		 * empties the resulting free entry.
		 *
		 * @param pos a starting position.
		 * @return the position cleared by the shifting process. */
		protected final int shiftKeys(int pos) {
			// Shift entries with the same hash.
			int last, slot;
			for (;;) {
				pos = ((last = pos) + 1) & mask;
				while (key[pos] != null) {
					slot = (int)(MurmurHash3.hash(key[pos]) & mask);
					if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) break;
					pos = (pos + 1) & mask;
				}
				if (key[pos] == null) break;
				key[last] = key[pos];
				value[last] = value[pos];
			}
			key[last] = null;
			return last;
		}

		public int remove(final byte[] array, final int offset, final int length, final long hash) {
			// The starting point.
			int pos = (int)(hash & mask);
			// There's always an unused entry.
			while (key[pos] != null) {
				if (key[pos].length == length && equals(key[pos], array, offset, length)) {
					size--;
					final int v = value[pos];
					shiftKeys(pos);
					return v;
				}
				pos = (pos + 1) & mask;
			}
			return 0;
		}

		public int get(final byte[] array, final int offset, final int length, final long hash) {
			// The starting point.
			int pos = (int)(hash & mask);
			// There's always an unused entry.
			while (key[pos] != null) {
				if (key[pos].length == length && equals(key[pos], array, offset, length)) return value[pos];
				pos = (pos + 1) & mask;
			}
			return 0;
		}

		/** Rehashes the stripe.
		 *
		 * @param newN the new size */
		protected void rehash(final int newN) {
			int i = 0, pos;
			final byte[][] key = this.key;
			final int[] value = this.value;
			final int mask = newN - 1; // Note that this is used by the hashing macro
			final byte[][] newKey = new byte[newN][];
			final int[] newValue = new int[newN];
			for (int j = size; j-- != 0;) {
				while (key[i] == null) i++;
				final byte[] k = key[i];
				pos = (int)(MurmurHash3.hash(k) & mask);
				while (newKey[pos] != null) pos = (pos + 1) & mask;
				newKey[pos] = k;
				newValue[pos] = value[i];
				i++;
			}
			n = newN;
			this.mask = mask;
			maxFill = 3 * (n / 4);
			this.key = newKey;
			this.value = newValue;
		}

		private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
			final byte key[][] = this.key;
			final int[] value = this.value;
			s.defaultWriteObject();
			for (int j = size, i = 0; j-- != 0; i++) {
				while (key[i] == null) i++;
				Util.writeVByte(key[i].length, s);
				s.write(key[i]);
				s.writeInt(value[i]);
			}
		}

		private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
			s.defaultReadObject();
			mask = n - 1;
			final byte[][] key = this.key = new byte[n][];
			final int[] value = this.value = new int[n];
			for (int i = size; i-- != 0;) {
				final int length = Util.readVByte(s);
				final byte[] k = new byte[length];
				s.readFully(k);
				final int v = s.readInt();
				int pos = (int)(MurmurHash3.hash(k) & mask);
				while (key[pos] != null) pos = (pos + 1) & mask;
				key[pos] = k;
				value[pos] = v;
			}
		}
	}

}
