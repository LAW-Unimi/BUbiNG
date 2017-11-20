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

import it.unimi.di.law.warc.processors.ParallelFilteredProcessorRunner.Writer;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.IOException;
import java.io.PrintStream;

public class URLPositionWriter implements Writer<WarcRecord> {
	private final long storeIndexMask;
	private final LongOpenHashSet repeatedSet;

	public URLPositionWriter(final String storeIndex, final String repeatedSetFile) throws ClassNotFoundException, IOException, IllegalArgumentException {
		this.storeIndexMask = (long)Integer.parseInt(storeIndex) << 48;
		this.repeatedSet = (LongOpenHashSet)BinIO.loadObject(repeatedSetFile);
	}

	@Override
	public void write(final WarcRecord warcRecord, final long storePosition, final PrintStream out) throws IOException {
		if (repeatedSet.contains(storeIndexMask | storePosition)) return;

		out.print(warcRecord.getWarcTargetURI());
		out.print('\t');
		out.print(storePosition);
		out.write('\n');
	}

	@Override
	public void close() throws IOException {}
}
