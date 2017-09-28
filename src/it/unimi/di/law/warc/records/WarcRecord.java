package it.unimi.di.law.warc.records;

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

import it.unimi.di.law.warc.util.BoundSessionInputBuffer;
import it.unimi.di.law.warc.util.ByteArraySessionOutputBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Date;
import java.util.UUID;

import org.apache.http.Header;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.HeaderGroup;

/**
 * An interface describing a WARC record.
 *
 * <p><strong>Required factory method</strong>:
 * a concrete type <code>T</code> implementing this interface <em>must</em> provide a factory method with signature
 * <pre>public static T fromPayload(final HeaderGroup warcHeaders, final BoundSessionInputBuffer payloadBuffer) throws IOException;</pre>
 * and update the {@link WarcRecord.Type} enum.
 *
 */
public interface WarcRecord {

	/** An enumeration of implemented record types. */
	public static enum Type {

		REQUEST("request"),
		RESPONSE("response"),
		WARCINFO("warcinfo");

		private final String value;

		final static Method REQUEST_READ_PAYLOAD;
		final static Method RESPONSE_READ_PAYLOAD;
		final static Method WARCINFO_READ_PAYLOAD;

		static {
			Method requestReadPayload = null;
			Method responseReadPayload = null;
			Method warcinfoReadPayload = null;
			try {
				requestReadPayload = HttpRequestWarcRecord.class.getMethod("fromPayload", HeaderGroup.class, BoundSessionInputBuffer.class);
				responseReadPayload = HttpResponseWarcRecord.class.getMethod("fromPayload", HeaderGroup.class, BoundSessionInputBuffer.class);
				warcinfoReadPayload = InfoWarcRecord.class.getMethod("fromPayload", HeaderGroup.class, BoundSessionInputBuffer.class);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			REQUEST_READ_PAYLOAD = requestReadPayload;
			RESPONSE_READ_PAYLOAD = responseReadPayload;
			WARCINFO_READ_PAYLOAD = warcinfoReadPayload;
		}

		Type(final String value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return this.value;
		}

		/**
		 * Creates the <code>WARC-Type</code> header of the given record type.
		 *
		 * @param type the record type.
		 * @return the header.
		 */
		public static Header warcHeader(final WarcRecord.Type type) {
			return new WarcHeader(WarcHeader.Name.WARC_TYPE, type.value);
		}

		/**
		 * Determines the WARC record type given the <code>WARC-Type</code> header.
		 *
		 * @param header the header.
		 * @return the record type.
		 */
		public static Type valueOf(final Header header) {
			if (! header.getName().equals(WarcHeader.Name.WARC_TYPE.value)) throw new IllegalArgumentException("Wrong header type " + header.getName());
			final String type = header.getValue();
			if (type.equals(RESPONSE.value))
				return RESPONSE;
			if (type.equals(REQUEST.value))
				return REQUEST;
			if (type.equals(WARCINFO.value))
				return WARCINFO;
			throw new IllegalArgumentException("Unrecognized type " + type);
		}

		/**
		 * Returns the factory method to be used to create a record from the payload given an header specifying the type.
		 *
		 * @param header the header.
		 * @return the factory method.
		 */
		public static Method fromPayloadMethod(final Header header) {
			final String type = header.getValue();
			if (type.equals(RESPONSE.value))
				return RESPONSE_READ_PAYLOAD;
			if (type.equals(REQUEST.value))
				return REQUEST_READ_PAYLOAD;
			if (type.equals(WARCINFO.value))
				return WARCINFO_READ_PAYLOAD;
			throw new IllegalArgumentException("Unrecognized type " + type);
		}

	}

	/** The version of the supported format. */
	public final static ProtocolVersion PROTOCOL_VERSION = new ProtocolVersion("WARC", 1, 0);

	/**
	 * Writes the WARC record.
	 *
	 * @param output the stream where to write the record.
	 * @param buffer a buffer that will be optionally used by the writer.
	 */
	public void write(final OutputStream output, final ByteArraySessionOutputBuffer buffer) throws IOException;

	/** Returns the WARC headers.
	 *
	 * @return the WARC headers.
	 */
	public HeaderGroup getWarcHeaders();

	/** Returns the specified WARC header.
	 *
	 * @param header the name of the header to return.
	 * @return the header, or <code>null</code> if the header is not present.
	 */
	public Header getWarcHeader(WarcHeader.Name header);

	// Typed getters for mandatory headers

	/**
	 * Returns the <code>WARC-Record-ID</code> header.
	 *
	 * @return the record UUID.
	 * @throws IllegalStateException in case the header (which is mandatory) is not present.
	 */
	public UUID getWarcRecordId();

	/**
	 * Returns the <code>WARC-Type</code> header.
	 *
	 * @return the record type.
	 * @throws IllegalStateException in case the header (which is mandatory) is not present.
	 */
	public Type getWarcType();

	/**
	 * Returns the <code>WARC-Date</code> header.
	 *
	 * @return the record creation date.
	 * @throws IllegalStateException in case the header (which is mandatory) is not present.
	 */
	public Date getWarcDate();

	/**
	 * Returns the WARC <code>Content-Length</code> header.
	 *
	 * @return the record payload length.
	 * @throws IllegalStateException in case the header (which is mandatory) is not present.
	 */
	public long getWarcContentLength();

	// Convenience getter for non-mandatory headers

	/**
	 * Returns the <code>WARC-Target-URI</code> header as a {@link URI}.
	 *
	 * <p>Note that different implementations might be more or less strict with respect to parsing.
	 * See, for instance, {@link AbstractWarcRecord#getWarcTargetURI()}.
	 *
	 * @return the record target URI.
	 * @throws IllegalStateException if the header is not present.
	 * @throws IllegalArgumentException if the header value cannot be parsed into a URI.
	 * @see AbstractWarcRecord#getWarcTargetURI()
	 */
	public URI getWarcTargetURI();
}
