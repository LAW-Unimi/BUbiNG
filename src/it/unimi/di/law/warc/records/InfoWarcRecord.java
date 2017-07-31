package it.unimi.di.law.warc.records;

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

import it.unimi.di.law.warc.io.WarcFormatException;
import it.unimi.di.law.warc.util.BoundSessionInputBuffer;
import it.unimi.di.law.warc.util.ByteArraySessionOutputBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.impl.io.AbstractMessageParser;
import org.apache.http.message.HeaderGroup;

/** An implementation of {@link WarcRecord} corresponding to a {@link WarcRecord.Type#WARCINFO} record type. */
public class InfoWarcRecord extends AbstractWarcRecord {

	private HeaderGroup info = new HeaderGroup();

	public InfoWarcRecord(final Header[] info) {
		this(null, info);
	}

	public static InfoWarcRecord fromPayload(final HeaderGroup warcHeaders, final BoundSessionInputBuffer payloadBuffer) throws IOException {
		return new InfoWarcRecord(warcHeaders, readPayload(payloadBuffer));
	}

	private InfoWarcRecord(final HeaderGroup warcHeaders, final Header[] info) {
		super(warcHeaders);
		this.warcHeaders.updateHeader(Type.warcHeader(Type.WARCINFO));
		this.info.setHeaders(info);
	}

	private static Header[] readPayload(final BoundSessionInputBuffer buffer) throws IOException {
		try {
			return AbstractMessageParser.parseHeaders(buffer, -1, -1, null);
		} catch (HttpException e) {
			throw new WarcFormatException("Can't parse information", e);
		}
	}

	public HeaderGroup getInfo() {
		return info;
	}

	@Override
	protected InputStream writePayload(final ByteArraySessionOutputBuffer buffer) throws IOException {
		writeHeaders(this.info, buffer);
		buffer.contentLength(buffer.size());
		return buffer.toInputStream();
	}

	@Override
	public String toString() {
		return
			"Warc headers: " + Arrays.toString(warcHeaders.getAllHeaders()) +
			"\nInfo headers: " + Arrays.toString(this.info.getAllHeaders());
	}

}
