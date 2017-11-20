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

import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.di.law.warc.io.WarcFormatException;
import it.unimi.di.law.warc.util.BoundSessionInputBuffer;
import it.unimi.di.law.warc.util.ByteArraySessionOutputBuffer;
import it.unimi.di.law.warc.util.HttpEntityFactory;
import it.unimi.di.law.warc.util.IdentityHttpEntityFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Locale;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.io.ContentLengthInputStream;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.impl.io.DefaultHttpResponseWriter;
import org.apache.http.message.HeaderGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HttpHeaders;

/** An implementation of {@link WarcRecord} corresponding to a {@link WarcRecord.Type#RESPONSE} record type. */
public class HttpResponseWarcRecord extends AbstractWarcRecord implements HttpResponse, URIResponse {

	private final static Logger LOGGER = LoggerFactory.getLogger(HttpResponseWarcRecord.class);

	public static final String HTTP_RESPONSE_MSGTYPE = "application/http;msgtype=response";

	private final ProtocolVersion protocolVersion;
	private final StatusLine statusLine;
	private final HttpEntity entity;

	/**
	 * Builds the record given the response and the target URI (using a {@link IdentityHttpEntityFactory} to store the entity in the record).
	 *
	 * @param targetURI the target URI.
	 * @param response the response.
	 */
	public HttpResponseWarcRecord(final URI targetURI, final HttpResponse response) throws IOException {
		this(null, targetURI, response, null);
	}

	/**
	 * Builds the record given the response, the target URI, and a {@link HttpEntityFactory}.
	 *
	 * @param targetURI the target URI.
	 * @param response the response.
	 * @param hef the {@link HttpEntityFactory} to be used to create the entity stored in the record, if {@code null} an {@link IdentityHttpEntityFactory} will be used.
	 */
	public HttpResponseWarcRecord(final URI targetURI, final HttpResponse response, final HttpEntityFactory hef) throws IOException {
		this(null, targetURI, response, hef);
	}

	public static HttpResponseWarcRecord fromPayload(final HeaderGroup warcHeaders, final BoundSessionInputBuffer payloadBuffer) throws IOException {
		return new HttpResponseWarcRecord(warcHeaders, null, readPayload(payloadBuffer), IdentityHttpEntityFactory.INSTANCE);
	}

	private HttpResponseWarcRecord(final HeaderGroup warcHeaders, final URI targetURI, final HttpResponse response, final HttpEntityFactory hef) throws IOException {
		super(targetURI, warcHeaders);
		getWarcTargetURI(); // Check correct initialization
		this.warcHeaders.updateHeader(Type.warcHeader(Type.RESPONSE));
		this.warcHeaders.updateHeader(new WarcHeader(WarcHeader.Name.CONTENT_TYPE, HTTP_RESPONSE_MSGTYPE));
		this.protocolVersion = response.getProtocolVersion();
		this.statusLine = response.getStatusLine();
		this.setHeaders(response.getAllHeaders());
		this.entity = (hef == null ? IdentityHttpEntityFactory.INSTANCE : hef).newEntity(response.getEntity());
	}

	private static HttpResponse readPayload(final BoundSessionInputBuffer buffer) throws IOException {
		final DefaultHttpResponseParser responseParser = new DefaultHttpResponseParser(buffer);
		final HttpResponse response;
		try {
			response = responseParser.parse();
		} catch (HttpException e) {
			throw new WarcFormatException("Can't parse the response", e);
		}
		final long remaining = buffer.remaining();

		if (LOGGER.isDebugEnabled()) { // This is just a check, the code up to the catch could be safely removed.
			final Header entityLengthHeader = response.getFirstHeader(HttpHeaders.CONTENT_LENGTH); // it may be different from WarcHeader.CONTENT_LENGTH since it's an HTTP header, not a WARC header
			if (entityLengthHeader != null) try {
				final long entityLength = Long.parseLong(entityLengthHeader.getValue());
				if (entityLength < remaining) LOGGER.debug("Content length header value {} is smaller than remaning bytes {}", Long.valueOf(entityLength), Long.valueOf(remaining));
				else if (entityLength > remaining) LOGGER.debug("Content length header value {} is greater than remaning bytes {} (this is probably due to truncation)", Long.valueOf(entityLength), Long.valueOf(remaining));
			} catch (NumberFormatException e) {}
		}

		final ContentLengthInputStream payload = new ContentLengthInputStream(buffer, remaining);
		final BasicHttpEntity entity = new BasicHttpEntity();
		entity.setContentLength(remaining);
		entity.setContent(payload);
		Header contentTypeHeader = response.getFirstHeader(HttpHeaders.CONTENT_TYPE);
		if (contentTypeHeader != null) entity.setContentType(contentTypeHeader);
		response.setEntity(entity);

		return response;
	}

	@Override
	public ProtocolVersion getProtocolVersion() {
		return this.protocolVersion;
	}

	@Override
	public StatusLine getStatusLine() {
		return this.statusLine;
	}

	@Override
	public HttpEntity getEntity() {
		return this.entity;
	}

	@Override
	protected InputStream writePayload(final ByteArraySessionOutputBuffer buffer) throws IOException {
		final DefaultHttpResponseWriter pw = new DefaultHttpResponseWriter(buffer);
		try {
			pw.write(this);
		} catch (HttpException e) {
			throw new RuntimeException("Unexpected HttpException.", e);
		}
		buffer.contentLength(buffer.size() + this.entity.getContentLength());
		return new SequenceInputStream(buffer.toInputStream(), this.entity.getContent()); // TODO: we never close the getContent() inputstream...
	}

	@Override
	public String toString() {
		return
			"Warc headers: " + Arrays.toString(warcHeaders.getAllHeaders()) +
			"\nResponse status line: " + this.statusLine +
			"\nResponse headers: " + Arrays.toString(this.getAllHeaders());
	}

	/* Unsupported mutability methods. */

	@Override
	public void setStatusLine(StatusLine statusline) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setStatusLine(ProtocolVersion ver, int code) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setStatusLine(ProtocolVersion ver, int code, String reason) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setStatusCode(int code) throws IllegalStateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setReasonPhrase(String reason) throws IllegalStateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setEntity(HttpEntity entity) {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public Locale getLocale() {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public void setLocale(Locale loc) {
		throw new UnsupportedOperationException();
	}

	@Override
	public URI uri() {
		return getWarcTargetURI();
	}

	@Override
	public HttpResponse response() {
		return this;
	}

}
