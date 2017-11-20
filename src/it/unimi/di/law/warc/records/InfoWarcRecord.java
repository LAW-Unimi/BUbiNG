package it.unimi.di.law.warc.records;

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
