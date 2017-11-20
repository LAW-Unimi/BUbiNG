package it.unimi.di.law.warc.util;

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

import java.util.Arrays;
import java.util.Locale;

import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EncodingUtils;
import org.apache.http.util.EntityUtils;

/** Mock implementations of some {@link AbstractHttpMessage}. */
public class StringHttpMessages {

	private static final ProtocolVersion PROTOCOL_VERSION = new ProtocolVersion("HTTP", 1, 1);

	/** A mock implementation of {@link org.apache.http.HttpRequest}. */
	public static class HttpRequest extends AbstractHttpMessage implements org.apache.http.HttpRequest {

		private final RequestLine requestLine;

		public HttpRequest(final String method, final String url) {
			this.requestLine = new BasicRequestLine(method, url, PROTOCOL_VERSION);
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

	/** A mock implementation of {@link org.apache.http.HttpResponse} using strings (based on {@link ByteArrayEntity}). */
	public static class HttpResponse extends AbstractHttpMessage implements org.apache.http.HttpResponse {

		private final StatusLine statusLine;
		private final HttpEntity entity;

		public HttpResponse(final int status, final String reason, final byte[] content, final int contentLength, final ContentType type) {
			this.statusLine = new BasicStatusLine(PROTOCOL_VERSION, status, reason);
			this.addHeader("Content-Length", Integer.toString(contentLength));
			this.addHeader("Content-Type", type.toString());
			this.entity = new ByteArrayEntity(content, 0, contentLength, type);
		}

		public HttpResponse(final String content) {
			this(200, "OK", content, ContentType.TEXT_HTML);
		}

		public HttpResponse(final String content, final ContentType type) {
			this(200, "OK", content, type);
		}

		public HttpResponse(final int status, final String reason, final String content, final ContentType type) {
			this(status, reason, EncodingUtils.getBytes(content, type.getCharset().toString()), type);
		}

		public HttpResponse(final int status, final String reason, final byte[] content, final ContentType type) {
			this(status, reason, content, content.length, type);
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

		@Override
		public String toString() {
			try {
				return "StatusLine: " + this.statusLine.toString() + "\nHeaders: " + Arrays.toString(this.getAllHeaders()) + "\nContent: " + EntityUtils.toString(this.entity);
			} catch (Exception  ingored) { return ""; }
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

