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
