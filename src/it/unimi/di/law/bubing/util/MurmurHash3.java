package it.unimi.di.law.bubing.util;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;

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

/** A 64-bit implementation of MurmurHash3 for byte-array fragments. */

public class MurmurHash3 {

	private static final long getblock(final byte[] key, final int i) {
		return ((key[i + 0] & 0x00000000000000FFL) << 0)
				| ((key[i + 1] & 0x00000000000000FFL) << 8)
				| ((key[i + 2] & 0x00000000000000FFL) << 16)
				| ((key[i + 3] & 0x00000000000000FFL) << 24)
				| ((key[i + 4] & 0x00000000000000FFL) << 32)
				| ((key[i + 5] & 0x00000000000000FFL) << 40)
				| ((key[i + 6] & 0x00000000000000FFL) << 48)
				| ((key[i + 7] & 0x00000000000000FFL) << 56);
	}

	private static final long fmix(long k) {
		k ^= k >>> 33;
		k *= 0xff51afd7ed558ccdL;
		k ^= k >>> 33;
		k *= 0xc4ceb9fe1a85ec53L;
		k ^= k >>> 33;
		return k;
	}

	/** Hashes a {@link ByteArrayList} using MurmurHash3 with seed zero.
	 *
	 * @param list a byte-array list
	 * @return a 64-bit MurmurHash3 zero-seed hash for the specified fragment. */

	public final static long hash(final ByteArrayList list) {
		return hash(list.elements(), 0, list.size());
	}


	/** Hashes a byte array using MurmurHash3 with seed zero.
	 *
	 * @param array a byte array.
	 * @return a 64-bit MurmurHash3 zero-seed hash for the specified fragment. */

	public final static long hash(final byte[] array) {
		return hash(array, 0, array.length);
	}

	/** Hashes a byte-array fragment using MurmurHash3 with seed zero.
	 *
	 * @param array a byte array.
	 * @param offset the first valid byte in {@code array}.
	 * @param length the number of valid elements in {@code array}.
	 * @return a 64-bit MurmurHash3 zero-seed hash for the specified fragment. */

	public final static long hash(final byte[] array, final int offset, final int length) {
		return hash(array, offset, length, 0);
	}

	/** Hashes a byte-array fragment using MurmurHash3.
	 *
	 * @param array a byte array.
	 * @param offset the first valid byte in {@code array}.
	 * @param length the number of valid elements in {@code array}.
	 * @param seed a seed.
	 * @return a 64-bit MurmurHash3 hash for the specified fragment. */

	public final static long hash(final byte[] array, final int offset, final int length, final long seed) {
		long h1 = 0x9368e53c2f6af274L ^ seed;
		long h2 = 0x586dcd208f7cd3fdL ^ seed;

		long c1 = 0x87c37b91114253d5L;
		long c2 = 0x4cf5ad432745937fL;

		long k1 = 0;
		long k2 = 0;

		for (int i = 0; i < length / 16; i++) {
			k1 = getblock(array, offset + i * 2 * 8);
			k2 = getblock(array, offset + (i * 2 + 1) * 8);

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
			k2 ^= (long)array[tail + 14] << 48;
		case 14:
			k2 ^= (long)array[tail + 13] << 40;
		case 13:
			k2 ^= (long)array[tail + 12] << 32;
		case 12:
			k2 ^= (long)array[tail + 11] << 24;
		case 11:
			k2 ^= (long)array[tail + 10] << 16;
		case 10:
			k2 ^= (long)array[tail + 9] << 8;
		case 9:
			k2 ^= (long)array[tail + 8] << 0;

		case 8:
			k1 ^= (long)array[tail + 7] << 56;
		case 7:
			k1 ^= (long)array[tail + 6] << 48;
		case 6:
			k1 ^= (long)array[tail + 5] << 40;
		case 5:
			k1 ^= (long)array[tail + 4] << 32;
		case 4:
			k1 ^= (long)array[tail + 3] << 24;
		case 3:
			k1 ^= (long)array[tail + 2] << 16;
		case 2:
			k1 ^= (long)array[tail + 1] << 8;
		case 1:
			k1 ^= (long)array[tail + 0] << 0;
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

		return h1;
	}
}
