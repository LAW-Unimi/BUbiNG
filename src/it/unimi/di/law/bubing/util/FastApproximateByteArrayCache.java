package it.unimi.di.law.bubing.util;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;

import java.util.concurrent.atomic.AtomicLong;

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

/** A fast, concurrent approximate cache for byte arrays.
 *
 * <p>This cache stores 128-bit MurmurHash3 fingerprints of byte arrays. Fingerprints are stored in
 * a number of stripes whose access is synchronized. Each stripe is an open-address linked hash table
 * similar to a {@link LongLinkedOpenHashSet}.
 *
 * <p>Fingerprints are evicted using a standard LRU strategy. Since we store fingerprints, it is in principle
 * possible to get false positives (i.e., {@link #add(byte[])} might return
 * {@code false} even if the argument was never added to the cache).
 *
 * <p>The number of objects created by this map is very small, and depends only
 * on the number of stripes, not on the size of the cache.
 *
 */

public class FastApproximateByteArrayCache {
	private final static int BYTES_PER_TABLE_CELL = 4 * Long.SIZE / Byte.SIZE;
	/** The stripes. Keys are distributed among them using the upper bits of their hash. */
	private final Stripe[] stripe;
	/** {@link #stripe map.length} &minus; 1, cached. */
	private final int mask;
	/** The number of cache hits. */
	private final AtomicLong hits;
	/** The number of cache misses. */
	private final AtomicLong misses;

	/** Creates a new cache with specified size and concurrency level equal to {@link Runtime#availableProcessors()}.
	 *
	 * @param byteSize the approximate size of the cache in bytes.
	 */
	public FastApproximateByteArrayCache(final long byteSize) {
		this(byteSize, Runtime.getRuntime().availableProcessors());
	}

	/** Creates a new cache with specified size.
	 *
	 * @param byteSize the approximate size of the cache in bytes.
	 * @param concurrencyLevel the number of stripes (it will be {@linkplain Integer#highestOneBit(int) forced to be a power of two}).
	 */
	public FastApproximateByteArrayCache(final long byteSize, final int concurrencyLevel) {
		hits = new AtomicLong();
		misses = new AtomicLong();
		stripe = new Stripe[Integer.highestOneBit(concurrencyLevel)];
		final long stripeSize = byteSize / stripe.length;
		if (stripeSize >= 2 * BYTES_PER_TABLE_CELL) {
			for (int i = stripe.length; i-- != 0;) stripe[i] = new Stripe(stripeSize, .75f);
			mask = stripe.length - 1;
		}
		else mask = -1; // Cache off
	}

	private static long getblock(final byte[] key, final int i) {
		return ((key[i + 0] & 0x00000000000000FFL) << 0)
				| ((key[i + 1] & 0x00000000000000FFL) << 8)
				| ((key[i + 2] & 0x00000000000000FFL) << 16)
				| ((key[i + 3] & 0x00000000000000FFL) << 24)
				| ((key[i + 4] & 0x00000000000000FFL) << 32)
				| ((key[i + 5] & 0x00000000000000FFL) << 40)
				| ((key[i + 6] & 0x00000000000000FFL) << 48)
				| ((key[i + 7] & 0x00000000000000FFL) << 56);
	}

	private static long fmix(long k) {
		k ^= k >>> 33;
		k *= 0xff51afd7ed558ccdL;
		k ^= k >>> 33;
		k *= 0xc4ceb9fe1a85ec53L;
		k ^= k >>> 33;
		return k;
	}

	public boolean add(final byte[] key) {
		return add(key, 0, key.length);
	}

	public boolean add(final ByteArrayList key) {
		return add(key.elements(), 0, key.size());
	}

	public boolean add(final byte[] key, final int offset, final int length) {

		if (mask == -1) { // Cache off
			misses.incrementAndGet();
			return true; // Zero
		}

		long h1 = 0x9368e53c2f6af274L;
		long h2 = 0x586dcd208f7cd3fdL;

		long c1 = 0x87c37b91114253d5L;
		long c2 = 0x4cf5ad432745937fL;

		long k1 = 0;
		long k2 = 0;

		for (int i = 0; i < length / 16; i++) {
			k1 = getblock(key, offset + i * 2 * 8);
			k2 = getblock(key, offset + (i * 2 + 1) * 8);

			k1 *= c1;
			k1 = (k1 << 23) | (k1 >>> 64 - 23);
			k1 *= c2;
			h1 ^= k1;
			h1 += h2;

			h2 = (h2 << 41) | (h2 >>> 64 - 41);

			k2 *= c2;
			k2 = (k2 << 23) | (k2 >>> 64 - 23);
			k2 *= c1;
			h2 ^= k2;
			h2 += h1;

			h1 = h1 * 3 + 0x52dce729;
			h2 = h2 * 3 + 0x38495ab5;

			c1 = c1 * 5 + 0x7b7d159c;
			c2 = c2 * 5 + 0x6bce6396;
		}

		k1 = 0;
		k2 = 0;

		final int tail = offset + ((length >>> 4) << 4);

		switch (length & 15) {
		case 15:
			k2 ^= (long)key[tail + 14] << 48;
		case 14:
			k2 ^= (long)key[tail + 13] << 40;
		case 13:
			k2 ^= (long)key[tail + 12] << 32;
		case 12:
			k2 ^= (long)key[tail + 11] << 24;
		case 11:
			k2 ^= (long)key[tail + 10] << 16;
		case 10:
			k2 ^= (long)key[tail + 9] << 8;
		case 9:
			k2 ^= (long)key[tail + 8] << 0;

		case 8:
			k1 ^= (long)key[tail + 7] << 56;
		case 7:
			k1 ^= (long)key[tail + 6] << 48;
		case 6:
			k1 ^= (long)key[tail + 5] << 40;
		case 5:
			k1 ^= (long)key[tail + 4] << 32;
		case 4:
			k1 ^= (long)key[tail + 3] << 24;
		case 3:
			k1 ^= (long)key[tail + 2] << 16;
		case 2:
			k1 ^= (long)key[tail + 1] << 8;
		case 1:
			k1 ^= (long)key[tail + 0] << 0;
			k1 *= c1;
			k1 = (k1 << 23) | (k1 >>> 64 - 23);
			k1 *= c2;
			h1 ^= k1;
			h1 += h2;

			h2 = (h2 << 41) | (h2 >>> 64 - 41);

			k2 *= c2;
			k2 = (k2 << 23) | (k2 >>> 64 - 23);
			k2 *= c1;
			h2 ^= k2;
			h2 += h1;

			h1 = h1 * 3 + 0x52dce729;
			h2 = h2 * 3 + 0x38495ab5;

			c1 = c1 * 5 + 0x7b7d159c;
			c2 = c2 * 5 + 0x6bce6396;
		}

		h2 ^= length;

		h1 += h2;
		h2 += h1;

		h1 = fmix(h1);
		h2 = fmix(h2);

		h1 += h2;
		h2 += h1;

		if (h1 == 0 && h2 == 0) h1 = -1; // We do not allow null fingerprints.

		final Stripe s = stripe[(int)(h1 & mask)];
		final boolean result;
		synchronized (s) {
			result = s.add(h1, h2);
		}
		if (result) misses.incrementAndGet();
		else hits.incrementAndGet();
		return result;
	}

	/** Returns the number of cache hits.
	 *
	 * @return the number of cache hits.
	 */
	public long hits() {
		return hits.get();
	}

	/** Returns the number of cache misses.
	 *
	 * @return the number of cache misses.
	 */
	public long misses() {
		return misses.get();
	}

	/** A class containing a stripe of the cache. */
	protected static final class Stripe {
		/** The array of signatures. Signatures are pairs of longs in position &lt;2<var>k</var>,2<var>k</var> + 1&gt;. */
		private final long key[];
		/** For each entry, the next and the previous entry in iteration order
		 * in position &lt;2<var>k</var>,2<var>k</var> + 1&gt;. The first entry
		 * has predecessor -1, and the last entry has successor -1. */
		private final int link[];
		/** Threshold after which we evict. */
		private final int maxFill;
		/** The mask for wrapping a key position counter. */
		private final int mask;
		/** Number of entries currently in the stripe. */
		private int size;
		/** The index of the first entry in LRU order. It is valid iff {@link #size} is nonzero; otherwise, it contains -1. */
		private int first = -1;
		/** The index of the last entry in LRU. It is valid iff {@link #size} is nonzero; otherwise, it contains -1. */
		private int last = -1;

		/** Creates a new stripe. */
		public Stripe(final long byteSize, final float f) {
			if (Long.highestOneBit(byteSize / (BYTES_PER_TABLE_CELL / 2)) >= (1 << 30)) throw new IllegalArgumentException();
			final int n = (int)Long.highestOneBit(byteSize / BYTES_PER_TABLE_CELL);
			maxFill = (int)(n * f);
			mask = n * 2 - 2;
			key = new long[n * 2];
			link = new int[n * 2];
		}

		public boolean add(final long lowerBitsKey, final long upperBitsKey) {
			// The starting point.
			int pos = (int)(upperBitsKey & mask); // Key array position
			// There's always an unused entry.
			while (key[pos] != 0 || key[pos + 1] != 0) {
				if (key[pos] == upperBitsKey && key[pos + 1] == lowerBitsKey) return false;
				pos = (pos + 2) & mask;
			}
			key[pos] = upperBitsKey;
			key[pos + 1] = lowerBitsKey;

			if (size == 0) {
				first = last = pos;
				link[pos] = -1;
				link[pos + 1] = -1;
			}
			else {
				link[last + 1] = pos;
				link[pos] = last;
				link[pos + 1] = -1;
				last = pos;
			}
			if (++size >= maxFill) removeLastRecentlyUsed();
			return true;
		}

		private void removeLastRecentlyUsed() {
			--size;
			final int pos = first; // Link array position
			first = link[pos + 1];
			if (0 <= first) link[first] = -1;
			shiftKeys(pos);
		}

		/** Shifts left entries with the specified hash code, starting at the specified position, and
		 * empties the resulting free entry.
		 *
		 * @param pos a key starting position.
		 * @return the position cleared by the shifting process. */
		private final int shiftKeys(int pos) {
			// Shift entries with the same hash.
			int last, slot;
			for (;;) {
				pos = ((last = pos) + 2) & mask;
				while (key[pos] != 0 || key[pos + 1] != 0) {
					slot = (int)(key[pos] & mask);
					if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) break;
					pos = (pos + 2) & mask;
				}
				if (key[pos] == 0 && key[pos + 1] == 0) break;
				key[last] = key[pos];
				key[last + 1] = key[pos + 1];
				fixPointers(pos, last);
			}
			key[last] = 0;
			key[last + 1] = 0;
			return last;
		}

		/** Modifies the {@link #link} vector for a shift from s to d.
		 *
		 * @param s the source link position.
		 * @param d the destination link position. */
		private void fixPointers(final int s, final int d) {
			if (size == 1) {
				first = last = d;
				link[d] = -1;
				link[d + 1] = -1;
				return;
			}
			if (first == s) {
				first = d;
				link[link[s + 1]] = d;
				link[d] = -1;
				link[d + 1] = link[s + 1];
				return;
			}
			if (last == s) {
				last = d;
				link[link[s] + 1] = d;
				link[d] = link[s];
				link[d + 1] = -1;
				return;
			}
			final int prev = link[s];
			final int next = link[s + 1];
			link[prev + 1] = d;
			link[next] = d ;
			link[d] = prev;
			link[d + 1] = next;
		}
	}
}
