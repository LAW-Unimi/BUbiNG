package it.unimi.di.law.warc.records;

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

//RELEASE-STATUS: DIST

import it.unimi.di.law.bubing.util.Util;

import java.util.Locale;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;


public class RandomTestMocks {

	public static final Random RNG = new Random();
	private static final ProtocolVersion PROTOCOL_VERSION = new ProtocolVersion("HTTP", 1, 1);

	private static Header[] randomHeaders(final int maxNum, final int maxLen) {
		int n = RNG.nextInt(maxNum) + 1;
		Header[] ret = new Header[n];
		for (int i = 0; i < n; i++) {
			String name = "Random-" + RandomStringUtils.randomAlphabetic(RNG.nextInt(maxLen) + 1);
			String value = RandomStringUtils.randomAscii(RNG.nextInt(maxLen) + 1);
			ret[i] = new BasicHeader(name, value);
		}
		return ret;
	}

	public static class HttpRequest extends AbstractHttpMessage implements org.apache.http.HttpRequest {

		private final RequestLine requestLine;

		public HttpRequest(final int maxNumberOfHeaders, final int maxLenghtOfHeader, final int pos) {
			this.requestLine = new BasicRequestLine("GET", RandomStringUtils.randomAlphabetic(RNG.nextInt(maxLenghtOfHeader) + 1), PROTOCOL_VERSION);
			Header[] headers = randomHeaders(maxNumberOfHeaders, maxLenghtOfHeader);
			headers[RNG.nextInt(headers.length)] = new BasicHeader("Position", Integer.toString(pos));
			this.setHeaders(headers);
		}

		@Override
		public ProtocolVersion getProtocolVersion() {
			return PROTOCOL_VERSION;
		}

		@Override
		public RequestLine getRequestLine() {
			return requestLine;
		}
	}

	public static class HttpResponse extends AbstractHttpMessage implements org.apache.http.HttpResponse {

		private final StatusLine statusLine;
		private final HttpEntity entity;
		private final String content;

		public HttpResponse(final int maxNumberOfHeaders, final int maxLenghtOfHeader, final int maxLengthOfBody, final int pos) {
			this.statusLine = new BasicStatusLine(PROTOCOL_VERSION, 200, "OK");
			Header[] headers = randomHeaders(maxNumberOfHeaders, maxLenghtOfHeader);
			headers[RNG.nextInt(headers.length)] = new BasicHeader("Position", Integer.toString(pos));
			this.setHeaders(headers);
			this.content = RandomStringUtils.randomAscii(RNG.nextInt(maxLengthOfBody) + 1);
			byte[] body = Util.toByteArray(content);
			this.addHeader("Content-Length", Integer.toString(body.length));
			this.entity = new ByteArrayEntity(body, ContentType.DEFAULT_BINARY);
		}

		public HttpResponse(final int maxNumberOfHeaders, final int maxLenghtOfHeader, String passedBody, final int pos) {
			this.statusLine = new BasicStatusLine(PROTOCOL_VERSION, 200, "OK");
			Header[] headers = randomHeaders(maxNumberOfHeaders, maxLenghtOfHeader);
			headers[RNG.nextInt(headers.length)] = new BasicHeader("Position", Integer.toString(pos));
			this.setHeaders(headers);
			this.content = passedBody;
			byte[] body = Util.toByteArray(content);
			this.addHeader("Content-Length", Integer.toString(body.length));
			this.entity = new ByteArrayEntity(body, ContentType.DEFAULT_BINARY);
		}

		public String getMockContent() {
			return this.content;
		}

		@Override
		public ProtocolVersion getProtocolVersion() {
			return PROTOCOL_VERSION;
		}

		@Override
		public StatusLine getStatusLine() {
			return this.statusLine;
		}

		@Override
		public HttpEntity getEntity() {
			return entity;
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

	}

}

