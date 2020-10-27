package it.unimi.di.law.warc.records;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.message.BasicLineFormatter;
import org.apache.http.message.HeaderGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

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

import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.Util;
import it.unimi.di.law.warc.io.WarcFormatException;
import it.unimi.di.law.warc.util.BoundSessionInputBuffer;
import it.unimi.di.law.warc.util.ByteArraySessionOutputBuffer;
import it.unimi.dsi.util.XoRoShiRo128PlusRandomGenerator;

/** An abstract implementation of a basic {@link WarcRecord}. */
public abstract class AbstractWarcRecord extends AbstractHttpMessage implements WarcRecord {
	public static final String USE_BURL_PROPERTY = "it.unimi.di.law.warc.records.useburl";
	private static final boolean USE_BURL = Boolean.parseBoolean(System.getProperty(USE_BURL_PROPERTY, "false"));

	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWarcRecord.class);
	private static final XoRoShiRo128PlusRandomGenerator RNG = new XoRoShiRo128PlusRandomGenerator();
	private static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");

	protected final HeaderGroup warcHeaders;

	/** BUilds a record, optionally given the warcHeaders.
	 *
	 * @param warcHeaders the WARC headers, may be {@code null}.
	 * @see AbstractWarcRecord#AbstractWarcRecord(URI,HeaderGroup)
	 */
	public AbstractWarcRecord(final HeaderGroup warcHeaders) {
		this(null, warcHeaders);
	}

	/** BUilds a record, optionally given the target URI and the warcHeaders.
	 *
	 * If the headers are {@code null} or the {@link WarcHeader.Name#WARC_RECORD_ID} header is absent, it will be generated at random,
	 * similarly if the headers are {@code null} or the {@link WarcHeader.Name#WARC_DATE} header absent, it will be set to the  current time.
	 * If the target URI is not {@code null} and the {@link WarcHeader.Name#WARC_TARGET_URI} header is not set, it will be set to the given vaule.
	 *
	 * @param targetURI the target URI, may be {@code null}.
	 * @param warcHeaders the WARC headers, may be {@code null}.
	 */
	public AbstractWarcRecord(final URI targetURI, final HeaderGroup warcHeaders) {
		this.warcHeaders = warcHeaders == null ? new HeaderGroup() : warcHeaders;
		final UUID id;
		synchronized (RNG) {
			id = new UUID(RNG.nextLong(), RNG.nextLong());
		}
		WarcHeader.addIfNotPresent(this.warcHeaders, WarcHeader.Name.WARC_RECORD_ID, WarcHeader.formatId(id));
		WarcHeader.addIfNotPresent(this.warcHeaders, WarcHeader.Name.WARC_DATE, WarcHeader.formatDate(Calendar.getInstance(UTC_TIMEZONE)));
		if (targetURI != null) WarcHeader.addIfNotPresent(this.warcHeaders, WarcHeader.Name.WARC_TARGET_URI, targetURI.toString()); // TODO: check with Seba that toString makes sense
	}

	@Override
	public ProtocolVersion getProtocolVersion() {
		return PROTOCOL_VERSION;
	}

	@Override
	public HeaderGroup getWarcHeaders() {
		return this.warcHeaders;
	}

	@Override
	public Header getWarcHeader(final WarcHeader.Name header) {
		return WarcHeader.getFirstHeader(this.warcHeaders, header);
	}

	@Override
	public UUID getWarcRecordId() {
		final Header header = WarcHeader.getFirstHeader(this.warcHeaders, WarcHeader.Name.WARC_RECORD_ID);
		if (header == null) throw new IllegalStateException(WarcHeader.Name.WARC_RECORD_ID + " mandatory header not present");
		UUID uuid;
		try {
			uuid = WarcHeader.parseId(header.getValue());
		} catch (final WarcFormatException e) {
			throw new IllegalStateException(WarcHeader.Name.WARC_RECORD_ID + " '" + header.getValue() + "' falied parsing", e);
		}
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Got UUID {}, parsed as {}", header.getValue(), uuid);
		return uuid;
	}

	@Override
	public Type getWarcType() {
		final Header header = WarcHeader.getFirstHeader(this.warcHeaders, WarcHeader.Name.WARC_TYPE);
		if (header == null) throw new IllegalStateException(WarcHeader.Name.WARC_TYPE + " mandatory header not present");
		return Type.valueOf(header);
	}

	@Override
	public Date getWarcDate() {
		final Header header = WarcHeader.getFirstHeader(this.warcHeaders, WarcHeader.Name.WARC_DATE);
		if (header == null) throw new IllegalStateException(WarcHeader.Name.WARC_DATE + " mandatory header not present");
		Date date = null;
		try {
			date = WarcHeader.parseDate(header.getValue());
		} catch (final WarcFormatException e) {
			throw new IllegalStateException(WarcHeader.Name.WARC_DATE + " '" + header.getValue() + "' falied parsing", e);
		}
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Got date {}, parsed as {}", header.getValue(), date);
		return date;
	}

	@Override
	public long getWarcContentLength() {
		final Header header = WarcHeader.getFirstHeader(this.warcHeaders, WarcHeader.Name.CONTENT_LENGTH);
		if (header == null) throw new IllegalStateException(WarcHeader.Name.CONTENT_LENGTH + " mandatory header not present");
		return Long.parseLong(header.getValue());
	}

	/**
	 * Returns the <code>WARC-Target-URI</code> header as a {@link URI}.
	 *
	 * <p>Parsing is performed by {@link URI#create(String)}, unless the system property {@value #USE_BURL_PROPERTY}
	 * has been set to true, in which case {@link BURL#parse(String)} will be used (if {@link BURL#parse(String)}
	 * returns {@code null}, we throw an {@link IllegalArgumentException} as from the specification
	 * in {@link WarcRecord#getWarcTargetURI()}).
	 *
	 * @return the record target URI.
	 * @throws IllegalStateException if the header is not present.
	 * @throws IllegalArgumentException if the header value cannot be parsed into a URI.
	 * @see AbstractWarcRecord#getWarcTargetURI()
	 */
	@Override
	public URI getWarcTargetURI() {
		final Header header = WarcHeader.getFirstHeader(this.warcHeaders, WarcHeader.Name.WARC_TARGET_URI);
		if (header == null) throw new IllegalStateException(WarcHeader.Name.WARC_TARGET_URI + " header not present");
		String value = header.getValue();
		if(value.startsWith("<") && value.endsWith(">")){
			// Handle wget-specific URI format
			value = value.substring(1, value.length() - 1);
		}
		if (! USE_BURL) return URI.create(header.getValue());
		final URI result = BURL.parse(header.getValue());
		if (result == null) throw new IllegalArgumentException("BURL.parse() found an unfixable syntax error in URL " + header.getValue());
		return result;
	}

	protected abstract InputStream writePayload(final ByteArraySessionOutputBuffer buffer) throws IOException;

	protected static void writeHeaders(final HeaderGroup headers, final OutputStream output) throws IOException {
		for (final HeaderIterator it = headers.iterator(); it.hasNext();) {
			final org.apache.http.Header header = it.nextHeader();
			Util.toOutputStream(BasicLineFormatter.formatHeader(header, null), output);
			output.write(ByteArraySessionOutputBuffer.CRLF);
		}
	}

	@Override
	public void write(OutputStream output, ByteArraySessionOutputBuffer buffer) throws IOException {

		buffer.reset();
		final InputStream payload = writePayload(buffer);
		final long contentLength = buffer.contentLength();

		this.warcHeaders.updateHeader(new WarcHeader(WarcHeader.Name.CONTENT_LENGTH, Long.toString(contentLength)));

		Util.toOutputStream(BasicLineFormatter.formatProtocolVersion(WarcRecord.PROTOCOL_VERSION, null), output);
		output.write(ByteArraySessionOutputBuffer.CRLF);
		writeHeaders(this.warcHeaders, output);
		output.write(ByteArraySessionOutputBuffer.CRLF);
		ByteStreams.copy(payload, output);
		output.write(ByteArraySessionOutputBuffer.CRLFCRLF);
	}

	public static WarcRecord fromPayload(final HeaderGroup warcHeaders, final BoundSessionInputBuffer payloadBuffer) throws IOException, WarcFormatException {
		final Header warcTypeHeader = WarcHeader.getFirstHeader(warcHeaders, WarcHeader.Name.WARC_TYPE);
		if (warcTypeHeader == null) throw new WarcFormatException("Missing 'WARC-Type' header");
		Method fromPayloadMethod = null;
		try {
			fromPayloadMethod = WarcRecord.Type.fromPayloadMethod(warcTypeHeader);
		} catch (final IllegalArgumentException e) {
			throw new WarcFormatException("Unrecognized record type", e);
		}
		try {
			return (WarcRecord) fromPayloadMethod.invoke(null, warcHeaders, payloadBuffer);
		} catch (final IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (final IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (final InvocationTargetException e) {
			throw new IOException(e);
		}
	}

}
