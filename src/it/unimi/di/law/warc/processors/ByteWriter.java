package it.unimi.di.law.warc.processors;

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

import it.unimi.di.law.warc.processors.ParallelFilteredProcessorRunner.Processor;
import it.unimi.di.law.warc.processors.ParallelFilteredProcessorRunner.Writer;

import java.io.IOException;
import java.io.PrintStream;

/** A writer that simply dumps to the output stream an array of bytes.
 *
 * <p>Note that this class does not perform any kind of line termination. It is up to the
 * {@link Processor} to provide line-terminated streams of bytes, if necessary.
 */

public class ByteWriter implements Writer<byte[]> {

	private static final ByteWriter INSTANCE = new ByteWriter();
	private ByteWriter() {}

	public static ByteWriter getInstance() {
		return INSTANCE;
	}

	@Override
	public void write(final byte[] b, final long storePosition, final PrintStream out) throws IOException {
		out.write(b);
	}

	@Override
	public void close() throws IOException {}
}
