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
