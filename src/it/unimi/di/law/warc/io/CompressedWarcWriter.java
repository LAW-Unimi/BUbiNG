package it.unimi.di.law.warc.io;

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

import it.unimi.di.law.warc.io.gzarc.GZIPArchive;
import it.unimi.di.law.warc.io.gzarc.GZIPArchiveWriter;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.ByteArraySessionOutputBuffer;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressedWarcWriter implements WarcWriter {

	private static final Logger LOGGER = LoggerFactory.getLogger(CompressedWarcWriter.class);

	private final GZIPArchiveWriter gzaw;
	private final ByteArraySessionOutputBuffer buffer;

	public CompressedWarcWriter(final OutputStream output) {
		this.gzaw = new GZIPArchiveWriter(output);
		this.buffer = new ByteArraySessionOutputBuffer();
	}

	@Override
	public void write(final WarcRecord record) throws IOException {
		GZIPArchive.WriteEntry e = gzaw.getEntry(record.getWarcRecordId().toString(), record.getWarcType().toString(), record.getWarcDate());
		record.write(e.deflater, this.buffer);
		e.deflater.close();
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Written {}", e);
	}

	@Override
	public void close() throws IOException {
		this.gzaw.close();
	}
}
