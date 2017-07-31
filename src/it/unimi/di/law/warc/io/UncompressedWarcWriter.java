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

import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.ByteArraySessionOutputBuffer;

import java.io.IOException;
import java.io.OutputStream;

public class UncompressedWarcWriter implements WarcWriter {

	private final OutputStream output;
	private final ByteArraySessionOutputBuffer buffer;

	public UncompressedWarcWriter(final OutputStream output) {
		this.output = output;
		this.buffer = new ByteArraySessionOutputBuffer();
	}

	@Override
	public void write(final WarcRecord record) throws IOException {
		record.write(this.output, this.buffer);
	}

	@Override
	public void close() throws IOException {
		this.output.close();
	}

}
