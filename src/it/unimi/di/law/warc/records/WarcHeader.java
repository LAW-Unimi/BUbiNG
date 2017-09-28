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

import it.unimi.di.law.warc.io.WarcFormatException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;

/**
 * A class used to represent WARC headers, with a set of static methods to handle them.
 */
@SuppressWarnings("serial")
public class WarcHeader extends BasicHeader {

	/** An enumeration of WARC headers. */
	public static enum Name {

		/* Mandatory */

		WARC_RECORD_ID("WARC-Record-ID"), // set by AbstractWarcRecord constructor
		WARC_DATE("WARC-Date"),			// set by AbstractWarcRecord constructor
		CONTENT_LENGTH("Content-Length"),	// set by AbstractWarcRecord.write
		WARC_TYPE("WARC-Type"),			// set by subclasses of AbstractWarcRecord

		/* Depending on type/case */

		CONTENT_TYPE("Content-Type"),
		WARC_CONCURRENT_TO(	"WARC-Concurrent-To"),
		WARC_BLOCK_DIGEST("WARC-Block-Digest"),
		WARC_PAYLOAD_DIGEST("WARC-Payload-Digest"),
		WARC_IP_ADDRESS("WARC-IP-Address"),
		WARC_REFERS_TO("WARC-Refers-To"),
		WARC_TARGET_URI("WARC-Target-URI"),						// set in HttpRequestWarcRecord and HttpResponseWarcRecord
		WARC_TRUNCATED("WARC-Truncated"),
		WARC_WARCINFO_ID("WARC-Warcinfo-ID"),
		WARC_IDENTIFIED_PAYLOAD_TYPE("WARC-Identified-Payload-Type"),
		WARC_SEGMENT_NUMBER("WARC-Segment-Number"),

		WARC_FILENAME("WARC-Filename"),							// only if warcinfo
		WARC_PROFILE("WARC-Profile"),								// only if revisit
		WARC_SEGMENT_ORIGIN_ID("WARC-Segment-Origin-ID"),			// only if continuation
		WARC_SEGMENT_TOTAL_LENGTH("WARC-Segment-Total-Length"),	// only if continuation

		/* BUbiNG headers */

		BUBING_GUESSED_CHARSET("BUbiNG-Guessed-Charset"),
		BUBING_IS_DUPLICATE("BUbiNG-Is-Duplicate");

		protected final String value;

		Name(final String value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return this.value;
		}
	};

	private final static DateFormat W3C_ISO8601_DATE_PARSE = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT);
	private final static FastDateFormat W3C_ISO8601_DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss'Z'");
	private final static String UUID_HEAD = "<urn:uid:";
	private final static int UUID_HEAD_LENGTH = UUID_HEAD.length();
	private final static String UUID_TAIL = ">";
	private final static int UUID_TAIL_LENGTH = UUID_TAIL.length();
	private final static String UUID_FORMAT = "<urn:uid:%s>";

	/** Creates a WARC header.
	 *
	 * @param name the header name.
	 * @param value the header value.
	 */
	public WarcHeader(final WarcHeader.Name name, final String value) {
		super(name.value, value);
	}

	/**
	 * Adds the given header, if not present (otherwise does nothing).
	 *
	 * @param headers the headers where to add the new one.
	 * @param name the name of the header to add.
	 * @param value the value of the header to add.
	 */
	public static void addIfNotPresent(final HeaderGroup headers, final WarcHeader.Name name, final String value) {
		if (! headers.containsHeader(name.value)) headers.addHeader(new WarcHeader(name, value));
	}

	/**
	 * Returns the first header of given name.
	 *
	 * @param headers the headers to search from.
	 * @param name the name of the header to lookup.
	 * @return the header.
	 */
	public static Header getFirstHeader(final HeaderGroup headers, final WarcHeader.Name name) {
		return headers.getFirstHeader(name.value);
	}

	/**
	 * Parses the date found in a {@link WarcHeader.Name#WARC_DATE} header.
	 *
	 * @param date the date.
	 * @return the parsed date.
	 */
	public static Date parseDate(final String date) throws WarcFormatException {
		try {
			synchronized (W3C_ISO8601_DATE_PARSE) {
				return W3C_ISO8601_DATE_PARSE.parse(date);
			}
		} catch (ParseException e) {
			throw new WarcFormatException("Error parsing date " + date, e);
		}
	}

	/**
	 * Formats the date to be written in the {@link WarcHeader.Name#WARC_DATE} header.
	 *
	 * @param calendar the date.
	 * @return the formatted date.
	 */
	public static String formatDate(final Calendar calendar) {
		return W3C_ISO8601_DATE_FORMAT.format(calendar);
	}

	/**
	 * Parses the date found in a {@link WarcHeader.Name#WARC_RECORD_ID} header.
	 *
	 * @param id the record id.
	 * @return the parsed record id.
	 */
	public static UUID parseId(final String id) throws WarcFormatException {
		if (! (id.startsWith(UUID_HEAD) && id.endsWith(UUID_TAIL))) throw new WarcFormatException("'" + id + "' wrong format for " + Name.WARC_RECORD_ID.value);
		final int len = id.length();
		UUID uuid;
		try {
			uuid = UUID.fromString(id.substring(UUID_HEAD_LENGTH, len - UUID_TAIL_LENGTH));
		} catch (IllegalArgumentException e) {
			throw new WarcFormatException("Error parsing uuid " + id, e);
		}
		return uuid;
	}

	/**
	 * Formats the record id to be written in the {@link WarcHeader.Name#WARC_RECORD_ID} header.
	 *
	 * @param id the record id.
	 * @return the formatted record id.
	 */
	public static String formatId(final UUID id) {
		return String.format(UUID_FORMAT, id.toString());
	}
}

