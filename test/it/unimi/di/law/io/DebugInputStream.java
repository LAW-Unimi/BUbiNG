package it.unimi.di.law.io;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;


//RELEASE-STATUS: DIST


/** An input stream that prints out some details for every call, used for debugging purposes. */

@SuppressWarnings("boxing")
public class DebugInputStream extends InputStream {

	private final String name;
	private final InputStream is;

	public DebugInputStream(String name, InputStream is) {
		this.name = name;
		this.is = is;
	}

	@Override
	public int available() throws IOException {
		final int available = is.available();
		System.err.printf(name + ": available() -> %d\n", available);
		return available;
	}

	@Override
	public void close() throws IOException {
		is.close();
	}

	@Override
	public void mark(int readlimit) {
		System.err.printf(name + ": mark(%d)\n", readlimit);
		is.mark(readlimit);
	}

	@Override
	public boolean markSupported() {
		return is.markSupported();
	}

	@Override
	public int read() throws IOException {
		final int read = is.read();
		System.err.printf(name + ": read() -> %d\n", read);
		return read;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		final int read = is.read(b, off, len);
		System.err.printf(name + ": read(-, %d, %d) -> %d, " + Arrays.toString(ArrayUtils.subarray(b, off, read < 0 ? 0 : read)) + "\n", off, len, read);
		return read;
	}

	@Override
	public void reset() throws IOException {
		System.err.println(name + ": reset()");
		is.reset();
	}

	@Override
	public long skip(long n) throws IOException {
		final long skip = is.skip(n);
		System.err.printf(name + ": skip(%d) -> %d\n", n, skip);
		return skip;
	}

}
