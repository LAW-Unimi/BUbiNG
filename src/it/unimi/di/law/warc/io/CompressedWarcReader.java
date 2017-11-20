package it.unimi.di.law.warc.io;

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
