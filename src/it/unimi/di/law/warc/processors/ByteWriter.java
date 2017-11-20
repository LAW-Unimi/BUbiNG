package it.unimi.di.law.warc.processors;

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
