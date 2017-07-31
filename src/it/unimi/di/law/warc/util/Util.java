package it.unimi.di.law.warc.util;

/*
 * Copyright (C) 2004-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
