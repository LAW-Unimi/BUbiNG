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

import it.unimi.di.law.warc.io.UncompressedWarcWriter;
import it.unimi.di.law.warc.processors.ParallelFilteredProcessorRunner.Writer;
import it.unimi.di.law.warc.records.WarcRecord;

import java.io.IOException;
import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;

/** A writer that simply writes the given record. */

public class IdentityWriter implements Writer<WarcRecord> {

	private static final Logger LOGGER =
			LoggerFactory.getLogger(IdentityWriter.class);
	private UncompressedWarcWriter writer;

	@Override
	public void write(final WarcRecord r, final long storePosition, final PrintStream out) throws IOException {
		if (writer == null) writer = new UncompressedWarcWriter(out);
		try {
			writer.write(r);
		} catch (Exception e) {
			LOGGER.error("Exception while writing record", e);
		}
	}

	@Override
	public void close() throws IOException {
		Closeables.close(writer, true);
	}
}
