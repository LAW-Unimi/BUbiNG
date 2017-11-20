package it.unimi.di.law.io;

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
