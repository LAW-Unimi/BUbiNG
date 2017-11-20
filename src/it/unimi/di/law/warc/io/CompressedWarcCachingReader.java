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

