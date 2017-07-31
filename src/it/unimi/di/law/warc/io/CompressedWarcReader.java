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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressedWarcReader extends AbstractWarcReader {
	private final static Logger LOGGER = LoggerFactory.getLogger(CompressedWarcReader.class);

	private final GZIPArchiveReader gzar;
	private GZIPArchive.ReadEntry previous;

	public CompressedWarcReader(final InputStream input) {
		this.gzar = new GZIPArchiveReader(input);
		this.previous = null;
	}

	@Override
	public WarcRecord read() throws IOException, WarcFormatException, GZIPArchive.FormatException {
		if (this.previous != null) {
			this.previous.lazyInflater.consume();
			if (LOGGER.isDebugEnabled()) LOGGER.debug("Consumed {}", this.previous);
		}
		final GZIPArchive.ReadEntry e = this.gzar.getEntry();
		if (e == null) {
			this.previous = null;
			return null;
		}
		this.previous = e;
		super.setInput(e.lazyInflater.get());
		return super.read(false);
	}

	@Override
	public void position(final long position) throws IOException {
		this.previous = null;
		this.gzar.position(position);
	}

}
