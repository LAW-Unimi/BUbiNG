package it.unimi.di.law.warc.util;

/*
 * Copyright (C) 2013-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.io.HttpTransportMetrics;
import org.apache.http.io.SessionOutputBuffer;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.CharArrayBuffer;

/** A {@link SessionOutputBuffer} implementation that uses a byte array as a backing store. */
public class ByteArraySessionOutputBuffer extends ByteArrayOutputStream implements SessionOutputBuffer {

	public static final byte[] CRLF = new byte[] { HTTP.CR, HTTP.LF };
	public static final byte[] CRLFCRLF = new byte[] { HTTP.CR, HTTP.LF, HTTP.CR, HTTP.LF };

	private long contentLength;

	public InputStream toInputStream() {
		return new ByteArrayInputStream(this.buf, 0, this.count);
	}

	public long contentLength() {
		return this.contentLength;
	}

	public void contentLength(final long contentLength) {
		this.contentLength = contentLength;
	}

	@Override
	public void writeLine(final String s) throws IOException {
		if (s == null) return;
		for (int i = 0; i < s.length(); i++) {
			write(s.charAt(i));
		}
		write(CRLF);
	}

	@Override
	public void writeLine(final CharArrayBuffer buffer) throws IOException {
		if (buffer == null) return;
		for (int i = 0; i < buffer.length(); i++) {
			write(buffer.charAt(i));
		}
		write(CRLF);
	}

	@Override
	public HttpTransportMetrics getMetrics() {
		throw new UnsupportedOperationException();
	}

}
