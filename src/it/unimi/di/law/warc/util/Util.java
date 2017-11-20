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

import it.unimi.dsi.lang.MutableString;

/** A class containing some utility functions. */
public class Util {

	/** Returns a string representing in hexadecimal a digest.
	 *
	 * @param a a digest, as a byte array.
	 * @return a string hexadecimal representation of <code>a</code>.
	 */
	public static String toHexString(final byte[] a) {
		MutableString result = new MutableString(a.length * 2);
		for (int i = 0; i < a.length; i++)
			result.append((a[i] >= 0 && a[i] < 16 ? "0" : "")).append(Integer.toHexString(a[i] & 0xFF));
		return result.toString();
	}

	/** Returns a byte array corresponding to the given number.
	 *
	 * @param s the number, as a String.
	 * @return the byte array.
	 */
	public static byte[] fromHexString(final String s) {
		byte[] b = new byte[s.length() / 2];
		for (int i = s.length() / 2; i-- != 0;)
			b[i] = (byte)Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
		return b;
	}

}
