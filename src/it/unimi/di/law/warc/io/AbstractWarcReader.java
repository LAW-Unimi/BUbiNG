package it.unimi.di.law.warc.io;

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

import it.unimi.di.law.warc.records.AbstractWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.BoundSessionInputBuffer;

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.ParseException;
import org.apache.http.ProtocolVersion;
import org.apache.http.impl.io.AbstractMessageParser;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.apache.http.io.SessionInputBuffer;
import org.apache.http.message.BasicLineParser;
import org.apache.http.message.HeaderGroup;
import org.apache.http.message.LineParser;
import org.apache.http.message.ParserCursor;
import org.apache.http.util.CharArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractWarcReader implements WarcReader {
	private static final boolean VERSION = Boolean.parseBoolean(System.getProperty("it.unimi.di.law.warc.io.version", "true"));
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWarcReader.class);

	private static final int BUFFER_SIZE = 1024;

	private final CharArrayBuffer line = new CharArrayBuffer(BUFFER_SIZE);
	private final HttpTransportMetricsImpl metrics = new HttpTransportMetricsImpl();
	private final LineParser parser = new BasicLineParser(WarcRecord.PROTOCOL_VERSION);

	private SessionInputBuffer buffer;
	private BoundSessionInputBuffer payload = null;

	protected void setInput(final InputStream input) {
		final SessionInputBufferImpl bufferImpl = new SessionInputBufferImpl(metrics, BUFFER_SIZE, 0, null, null);
		bufferImpl.bind(input);
		this.buffer = bufferImpl;
		this.payload = null;
	}

	private ProtocolVersion parseHead() throws IOException  {
		this.line.clear();
		int read = this.buffer.readLine(this.line);
		if (LOGGER.isTraceEnabled()) LOGGER.trace("Protocol header '{}'.", new String(this.line.toCharArray()));
		if (read == -1) return null;
		ParserCursor cursor = new ParserCursor(0, this.line.length());
		try {
			return parser.parseProtocolVersion(this.line, cursor);
		} catch (ParseException e) {
			throw new WarcFormatException("Can't parse WARC version header.", e);
		}
	}

	protected WarcRecord read(final boolean consecutive) throws IOException, WarcFormatException {

		if (consecutive && this.payload != null) {
			this.payload.consume();
			this.payload = null;

			this.line.clear();
			this.buffer.readLine(this.line);
			this.buffer.readLine(this.line);
			if (line.length() != 0) throw new WarcFormatException("Missing CRLFs at WARC record end, got \"" + line + "\"");
			this.line.clear();
		}

		// first header line

		final ProtocolVersion version = parseHead();
		if (version == null) return null;
		if (VERSION && (version.getMajor() != 1 || version.getMinor() != 0)) throw new IllegalArgumentException("Unsupported WARC version " + version);

		// rest of headers

		final HeaderGroup warcHeaders = new HeaderGroup();
		try {
			warcHeaders.setHeaders(AbstractMessageParser.parseHeaders(this.buffer, -1, -1, null));
		} catch (HttpException e) {
			throw new WarcFormatException("Can't parse WARC headers", e);
		}

		// payload

		final Header payloadLengthHeader = WarcHeader.getFirstHeader(warcHeaders, WarcHeader.Name.CONTENT_LENGTH);
		if (payloadLengthHeader == null) throw new WarcFormatException("Missing 'Content-Length' WARC header");
		long payloadLength = -1;
		try {
			payloadLength = Long.parseLong(payloadLengthHeader.getValue());
		} catch (NumberFormatException e) {
			throw new WarcFormatException("Can't parse 'Content-Length' WARC header (is \"" + payloadLengthHeader.getValue() +"\")", e);
		}
		this.payload = new BoundSessionInputBuffer(this.buffer, payloadLength);

		return AbstractWarcRecord.fromPayload(warcHeaders, this.payload);
	}

}
