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
