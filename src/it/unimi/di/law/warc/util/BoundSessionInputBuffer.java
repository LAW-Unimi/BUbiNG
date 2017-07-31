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

import java.io.IOException;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.http.impl.io.ContentLengthInputStream;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.apache.http.io.SessionInputBuffer;

/**
 * A {@link SessionInputBuffer} implementation that bounds a {@link SessionInputBuffer} (and hence its
 * buffered stream) so that no more than a specified amount of bytes will be read (from its stream), and
 * keeps track of the number of read bytes.
 */
public class BoundSessionInputBuffer extends SessionInputBufferImpl {

	private static final int BUFFER_SIZE = 1024;

	/** A stream wrapping the passed buffer, used to limit the reads to the given length. */
	private final ContentLengthInputStream bounded;

	/** A stream wrapping the {@link #bounded} stream, to keep track of read bytes. */
	private final CountingInputStream input;

	/** The maximum number of bytes that can be read. */
	private final long length;

	/**
	 * Creates a new {@link SessionInputBuffer} bounded to a given maximum length.
	 *
	 * @param buffer the buffer to wrap
	 * @param length the maximum number of bytes to read (from the buffered stream).
	 */
	public BoundSessionInputBuffer(final SessionInputBuffer buffer, final long length) {
		super(new HttpTransportMetricsImpl(), BUFFER_SIZE, 0, null, null);
		this.bounded = new ContentLengthInputStream(buffer, length);
		this.input = new CountingInputStream(this.bounded);
		super.bind(this.input);
		this.length = length;
	}

	/**
	 * Returns the number of unread bytes (in the buffered stream).
	 *
	 * @return the number of unread bytes.
	 */
	public long remaining() {
		return length - this.input.getByteCount() + length();
	}

	/**
	 * Consumes the remaining bytes (of the buffered stream).
	 */
	public void consume() throws IOException {
		this.bounded.skip(Long.MAX_VALUE);
	}

}
