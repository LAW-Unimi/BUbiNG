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
import it.unimi.dsi.fastutil.io.RepositionableStream;

import java.io.IOException;
import java.io.InputStream;

public class UncompressedWarcReader extends AbstractWarcReader {

	private final RepositionableStream repositionableInput;
	private boolean consecutive;

	public UncompressedWarcReader(final InputStream input) {
		super();
		this.repositionableInput = input instanceof RepositionableStream ? (RepositionableStream)input : null;
		this.consecutive = true;
		super.setInput(input);
	}

	@Override
	public WarcRecord read() throws IOException, WarcFormatException {
		final WarcRecord record = super.read(this.consecutive);
		this.consecutive = true;
		return record;
	}


	@Override
	public void position(final long position) throws IOException {
		if (this.repositionableInput == null) throw new UnsupportedOperationException();
		this.repositionableInput.position(position);
		this.consecutive = false;
	}

}
