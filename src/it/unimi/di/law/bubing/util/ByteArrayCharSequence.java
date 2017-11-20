package it.unimi.di.law.bubing.util;

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

import it.unimi.dsi.fastutil.bytes.ByteArrays;

// RELEASE-STATUS: DIST

/** An adapter exposing a byte array as an ISO-8859-1-encoded
 * character sequence.
 *
 * <p>An instance of this adapter can be reused by {@linkplain #wrap(byte[], int, int)
 * wrapping a new byte array}.
 *
 * <p>Note that for convenience this class exposes a {@link #hashCode()} method that
 * return the same result as {@link String}, but equality is not by content.
 */

public class ByteArrayCharSequence implements CharSequence {
	/** The underlying byte array. */
	private byte[] b;
	/** The first valid byte in {@link #b}. */
	private int offset;
	/** The number of valid bytes in {@link #b}, starting at {@link #offset}. */
	private int length;

	/** Creates a new byte-array character sequence using the provided byte-array fragment.
	 *
	 * @param b a byte array.
	 * @param offset the first valid byte in <code>b</code>.
	 * @param length the number of valid bytes in <code>b</code>, starting at <code>offset</code>.
	 */
	public ByteArrayCharSequence(final byte[] b, int offset, int length) {
		wrap(b, offset, length);
	}

	/** Creates a new byte-array character sequence using the provided byte array.
	 *
	 * @param b a byte array.
	 */
	public ByteArrayCharSequence(final byte[] b) {
		this(b, 0, b.length);
	}

	/** Creates a new empty byte-array character sequence.
	 */
	public ByteArrayCharSequence() {
		this(ByteArrays.EMPTY_ARRAY);
	}

	/** Wraps a byte-array fragment into this byte-array character sequence.
	 *
	 * @param b a byte array.
	 * @param offset the first valid byte in <code>b</code>.
	 * @param length the number of valid bytes in <code>b</code>, starting at <code>offset</code>.
	 * @return this byte-array character sequence.
	 */
	public ByteArrayCharSequence wrap(final byte[] b, int offset, int length) {
		ByteArrays.ensureOffsetLength(b, offset, length);
		this.b = b;
		this.offset = offset;
		this.length = length;
		return this;
	}

	/** Wraps a byte array into this byte-array character sequence.
	 *
	 * @param b a byte array.
	 */
	public void wrap(final byte[] b) {
		wrap(b, 0, b.length);
	}

	@Override
	public int length() {
		return length;
	}

	@Override
	public char charAt(int index) {
		if (index < 0 || index >= length) throw new IndexOutOfBoundsException(Integer.toString(index));
		return (char)(b[offset + index] & 0xFF);
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		if (start < 0 || end > length || end < 0 || end < start) throw new IndexOutOfBoundsException();
		return new ByteArrayCharSequence(b, start + offset, end - start);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		for(int i = 0; i < length; i++) builder.append((char)(b[offset + i] & 0xFF));
		return builder.toString();
	}

	@Override
    public int hashCode() {
    	int h = 0;
    	for (int i = 0; i < length; i++) h = 31 * h + b[offset + i];
        return h;
    }
}
