package it.unimi.di.law.warc.util;

/*
 * Copyright (C) 2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
