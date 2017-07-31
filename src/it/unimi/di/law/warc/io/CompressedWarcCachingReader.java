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
import it.unimi.di.law.warc.io.gzarc.GZIPArchiveReader;
import it.unimi.di.law.warc.records.WarcRecord;

import java.io.IOException;
import java.io.InputStream;

public class CompressedWarcCachingReader implements WarcCachingReader {

	private final GZIPArchiveReader gzar;

	public CompressedWarcCachingReader(final InputStream input) {
		this.gzar = new GZIPArchiveReader(input);
	}

	@Override
	public void position(final long position) throws IOException {
		this.gzar.position(position);
	}

	@Override
	public WarcReader cache() throws IOException {

		final GZIPArchive.ReadEntry e = this.gzar.getEntry(true);
		if (e == null) return null;

		// for cached entry it's ok to consume before get
		e.lazyInflater.consume();

		return new AbstractWarcReader() {

			@Override
			public WarcRecord read() throws IOException, WarcFormatException {
				super.setInput(e.lazyInflater.get());
				return super.read(false);
			}

			@Override
			public void position(final long position) throws IOException {
				throw new UnsupportedOperationException();
			}


		};

	}

}

