package it.unimi.di.law.bubing.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.commons.lang.BooleanUtils;

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


import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

//RELEASE-STATUS: DIST

/** Generic static utility method container.  */

public class Util {

	/** The random number generator used by {@link #createHierarchicalTempFile(File, int)}. */
	private static final XoRoShiRo128PlusRandom RND = new XoRoShiRo128PlusRandom();
	/** The lock used by {@link #createHierarchicalTempFile(File, int, String, String)} when creating files. */
	private static final Object CREATION_LOCK = new Object();


	/** Parses a Boolean value reliably, throwing an exception if the argument is not
	 * {@code true} or {@code false} (case insensitively).
	 *
	 * @param s a string containing {@code true} or {@code false}.
	 * @return true or false, depending on the content of {@code s}.
	 */
	public static boolean parseBoolean(String s) {
		return BooleanUtils.toBoolean(s.toLowerCase(), "true", "false");
	}

	/**
	 * Creates a temporary file with a random hierachical path.
	 *
	 * <p> A random hierarchical path of <var>n</var> path elements is a sequence of <var>n</var>
	 * directories of two hexadecimal digits each, followed by a filename created by {@link File#createTempFile(String, String, File)}.
	 *
	 * <p> This method creates an empty file having a random hierarchical path of the specified
	 * number of path elements under a given base directory, creating all needed directories along
	 * the hierarchical path (whereas the base directory is expected to already exist).
	 *
	 * @param baseDirectory the base directory (it must exist).
	 * @param pathElements the number of path elements (filename excluded), must be in [0,8]
	 * @param prefix will be passed to {@link File#createTempFile(String, String, File)}
	 * @param suffix will be passed to {@link File#createTempFile(String, String, File)}
	 * @return the temporary file.
	 * @throws IOException
	 */
	public static File createHierarchicalTempFile(final File baseDirectory, final int pathElements, final String prefix, final String suffix) throws IOException {
		if (! baseDirectory.isDirectory()) throw new IllegalArgumentException(baseDirectory + " is not a directory.");
		if (pathElements < 0 || pathElements > 8) throw new IllegalArgumentException();

		long x;
		synchronized (RND) { x = RND.nextLong(); }
		final StringBuilder stringBuilder = new StringBuilder();
		for(int i = 0; i < pathElements; i++) {
			if (i != 0) stringBuilder.append(File.separatorChar);
			stringBuilder.append(Long.toHexString(x & 0xF));
			x >>= 4;
			stringBuilder.append(Long.toHexString(x & 0xF));
			x >>= 4;
		}

		File directory = baseDirectory;
		if (pathElements > 0) {
			directory = new File(baseDirectory, stringBuilder.toString());
			synchronized (CREATION_LOCK) {
				if ((directory.exists() && ! directory.isDirectory()) || (! directory.exists() && ! directory.mkdirs())) throw new IOException("Cannot create directory " + directory);
			}
		}

		return File.createTempFile(prefix, suffix, directory);
	}

	/** Returns the length of the vByte encoding of a natural number.
	 *
	 * @param x a natural number.
	 * @return the length of the vByte encoding of {@code x}.
	 */
	public static final int vByteLength(final int x) {
		return Fast.mostSignificantBit(x) / 7 + 1;
	}

	/** Encodes a natural number to an {@link OutputStream} using vByte.
	 *
	 * @param x a natural number.
	 * @param os an output stream.
	 * @return the number of bytes written.
	 */
	public static int writeVByte(final int x, final OutputStream os) throws IOException {
		if (x < (1 << 7)) {
			os.write(x);
			return 1;
		}

		if (x < (1 << 14)) {
			os.write((x >>> 7 | 0x80));
			os.write(x & 0x7F);
			return 2;
		}

		if (x < (1 << 21)) {
			os.write((x >>> 14 | 0x80));
			os.write((x >>> 7 | 0x80));
			os.write(x & 0x7F);
			return 3;
		}

		if (x < (1 << 28)) {
			os.write((x >>> 21 | 0x80));
			os.write((x >>> 14 | 0x80));
			os.write((x >>> 7 | 0x80));
			os.write(x & 0x7F);
			return 4;
		}

		os.write((x >>> 28 | 0x80));
		os.write((x >>> 21 | 0x80));
		os.write((x >>> 14 | 0x80));
		os.write((x >>> 7 | 0x80));
		os.write(x & 0x7F);
		return 5;
	}

	/** Decodes a natural number from an {@link InputStream} using vByte.
	 *
	 * @param is an input stream.
	 * @return a natural number decoded from {@code is} using vByte.
	 */
	public static int readVByte(final InputStream is) throws IOException {
		for(int x = 0; ;) {
			final int b = is.read();
			x |= b & 0x7F;
			if ((b & 0x80) == 0) return x;
			x <<= 7;
		}
	}

	/** Writes a byte array prefixed by its length encoded using vByte.
	 *
	 * @param a the array to be written.
	 * @param s the stream where the array should be written.
	 */
	public final static void writeByteArray(final byte[] a, final ObjectOutputStream s) throws IOException {
		writeVByte(a.length, s);
		s.write(a);
	}

	/** Reads a byte array prefixed by its length encoded using vByte.
	 *
	 * @param s the stream from which the array should be read.
	 * @return the array.
	 */
	public final static byte[] readByteArray(final ObjectInputStream s) throws IOException {
		final byte[] a = new byte[readVByte(s)];
		s.readFully(a);
		return a;
	}

	/** Returns a byte-array representation of an ASCII string.
	 *
	 * <p>This method is significantly faster than those relying on character encoders, and it allocates just
	 * one object&mdash;the resulting byte array.
	 *
	 * @param s an ASCII string.
	 * @return a byte-array representation of {@code s}.
	 * @throws AssertionError if assertions are enabled and some character of {@code s} is not ASCII.
	 */
	public static byte[] toByteArray(final String s) {
		final byte[] byteArray = new byte[s.length()];
		// This needs to be fast.
		for(int i = s.length(); i-- != 0;) {
			assert s.charAt(i) < (char)0x80 : s.charAt(i);
			byteArray[i] = (byte)s.charAt(i);
		}
		return byteArray;
	}

	/** Writes a string to an output stream, discarding higher order bits.
	 *
	 * <p>This method is significantly faster than those relying on character encoders, and it does not allocate any object.
	 *
	 * @param s a string.
	 * @param os an output stream.
	 * @return {@code os}.
	 * @throws AssertionError if assertions are enabled and some character of {@code s} does not fit a byte.
	 */
	public static OutputStream toOutputStream(final String s, final OutputStream os) throws IOException {
		// This needs to be fast.
		final int length = s.length();
		for(int i = 0; i < length; i++) {
			assert s.charAt(i) < (char)0x100 : s.charAt(i);
			os.write(s.charAt(i));
		}

		return os;
	}


	/** Writes an ASCII string in a {@link ByteArrayList}.
	 *
	 * @param s an ASCII string.
	 * @param list a byte list that will contain the byte representation of {@code s}.
	 * @return {@code list}.
	 * @throws AssertionError if assertions are enabled and some character of {@code s} is not ASCII.
	 */
	public static ByteArrayList toByteArrayList(final String s, final ByteArrayList list) {
		list.clear();
		list.size(s.length());
		final byte[] array = list.elements();
		for (int i = list.size(); i-- != 0;) {
			assert s.charAt(i) < (char)0x80 : s.charAt(i);
			array[i] = (byte)(s.charAt(i) & 0x7F);
		}
		return list;
	}

	/** Returns a string representation of an ASCII byte array.
	 *
	 * <p>This method is significantly faster than those relying on character encoders, and it allocates just
	 * one object&mdash;the resulting string.
	 *
	 * @param byteArrayList an ASCII byte-array list.
	 * @return a string representation of {@code byteArrayList}.
	 * @throws AssertionError if assertions are enabled and some character of {@code byteArrayList} is not ASCII.
	 */
	public static String toString(final ByteArrayList byteArrayList) {
		final char[] charArray = new char[byteArrayList.size()];
		final byte[] byteArray = byteArrayList.elements();
		// This needs to be fast.
		for(int i = byteArrayList.size(); i-- != 0;) {
			assert byteArray[i] < (char)0x80 : byteArray[i];
			charArray[i] = (char)byteArray[i];
		}
		return new String(charArray);
	}

	/** Returns a string representation of an ASCII byte array.
	 *
	 * <p>This method is significantly faster than those relying on character encoders, and it allocates just
	 * one object&mdash;the resulting string.
	 *
	 * @param byteArray an ASCII byte array.
	 * @return a string representation of {@code byteArray}.
	 * @throws AssertionError if assertions are enabled and some character of {@code byteArray} is not ASCII.
	 */
	public static String toString(final byte[] byteArray) {
		final char[] charArray = new char[byteArray.length];
		// This needs to be fast.
		for(int i = charArray.length; i-- != 0;) {
			assert byteArray[i] < (char)0x80 : byteArray[i];
			charArray[i] = (char)byteArray[i];
		}
		return new String(charArray);
	}

	/** Returns a string representation of an ASCII byte-array fragment.
	 *
	 * <p>This method is significantly faster than those relying on character encoders, and it allocates just
	 * one object&mdash;the resulting string.
	 *
	 * @param byteArray an ASCII byte array.
	 * @return a string representation of {@code byteArray}.
	 * @throws AssertionError if assertions are enabled and some character of {@code byteArray} is not ASCII.
	 */
	public static String toString(final byte[] byteArray, final int offset, final int length) {
		final char[] charArray = new char[length];
		// This needs to be fast.
		for(int i = charArray.length; i-- != 0;) {
			assert byteArray[offset + i] < (char)0x80 : byteArray[offset + i];
			charArray[i] = (char)byteArray[offset + i];
		}
		return new String(charArray);
	}



}
