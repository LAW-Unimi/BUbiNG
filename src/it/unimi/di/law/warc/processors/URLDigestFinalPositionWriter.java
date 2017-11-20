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
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader.Name;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.IOException;
import java.io.PrintStream;

public class URLDigestFinalPositionWriter implements Writer<WarcRecord> {
	/** The final position in the deduplicated set of stores. */
	private long finalPosition;

	private final long storeIndexMask;
	private final LongOpenHashSet repeatedSet;

	public URLDigestFinalPositionWriter(final String storeIndex, final String repeatedSetFile) throws ClassNotFoundException, IOException, IllegalArgumentException {
		this.storeIndexMask = (long)Integer.parseInt(storeIndex) << 48;
		this.repeatedSet = (LongOpenHashSet)BinIO.loadObject(repeatedSetFile);
	}


	@Override
	public void write(final WarcRecord warcRecord, final long storePosition, final PrintStream out) throws IOException {
		if (repeatedSet.contains(storeIndexMask | storePosition)) return;

		final boolean isDuplicate = warcRecord.getWarcHeader(Name.BUBING_IS_DUPLICATE) != null;

		out.print(warcRecord.getWarcTargetURI());
		out.print('\t');
		out.print(warcRecord.getWarcHeader(Name.WARC_PAYLOAD_DIGEST).getValue());
		out.print('\t');
		out.print(isDuplicate ? -1 : finalPosition++);
		out.write('\t');
		out.print(((HttpResponseWarcRecord)warcRecord).getStatusLine());
		out.write('\n');
	}

	@Override
	public void close() throws IOException {}
}
