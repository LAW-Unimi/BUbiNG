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

//RELEASE-STATUS: DIST

import it.unimi.di.law.warc.io.WarcFormatException;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.impl.io.ContentLengthInputStream;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.apache.http.io.SessionInputBuffer;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.apache.http.util.CharArrayBuffer;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

public class InputStreamTestMocks {

	public final static Set<String> EMPTY_SET_OF_STRINGS = Collections.emptySet();
	public final static List<Set<String>> EMPTY_DIFFS = Arrays.asList(EMPTY_SET_OF_STRINGS, EMPTY_SET_OF_STRINGS);
	public final static List<Set<String>> ID_DIFFS = Arrays.asList(Sets.newHashSet("warc-record-id"), EMPTY_SET_OF_STRINGS);
	public final static List<Set<String>> ID_DATE_DIFFS = Arrays.asList(Sets.newHashSet("warc-record-id", "warc-date"), EMPTY_SET_OF_STRINGS);

	public static long contentLength(String[] read) {
		for (String s : read) {
			String[] p = s.split(":", 2);
			if (p.length != 2) continue;
			if (p[0].trim().toLowerCase().equals("content-length"))
				try {
					return Long.parseLong(p[1].trim());
				} catch (NumberFormatException e) {
					return -1;
				}
		}
		return -1;
	}

	public static Set<String> keys(HeaderGroup hg) {
		Set<String> ret = new HashSet<>();
		for (HeaderIterator it = hg.iterator(); it.hasNext();) {
			Header header = it.nextHeader();
			ret.add(header.getName().toLowerCase());
		}
		return ret;
	}

	public static List<Set<String>> diff(HeaderGroup expected, HeaderGroup actual) {
		Set<String> expecetdKeys = keys(expected);
		Set<String> actualKeys = keys(actual);
		Set<String> common = Sets.intersection(expecetdKeys, actualKeys);
		Set<String> symdiff = Sets.symmetricDifference(expecetdKeys, actualKeys);
		Set<String> diffval = new HashSet<>();
		for (String s : common)
			if (! expected.getCondensedHeader(s).getValue().equals(actual.getCondensedHeader(s).getValue())) diffval.add(s);
		return Arrays.asList(diffval, symdiff);
	}

	public static List<Set<String>> diff(Header[] expecteds, HeaderGroup actual) {
		HeaderGroup expected = new HeaderGroup();
		expected.setHeaders(expecteds);
		return diff(expected, actual);
	}

	public static List<Set<String>> diff(HeaderGroup expected, Header[] actuals) {
		HeaderGroup actual = new HeaderGroup();
		actual.setHeaders(actuals);
		return diff(expected, actual);
	}

	public static List<Set<String>> diff(Header[] expecteds, Header[] actuals) {
		HeaderGroup expected = new HeaderGroup();
		expected.setHeaders(expecteds);
		HeaderGroup actual = new HeaderGroup();
		expected.setHeaders(actuals);
		return diff(expected, actual);
	}

	public static SessionInputBuffer warpSessionInputBuffer(final InputStream input) {
		final SessionInputBufferImpl bufferImpl = new SessionInputBufferImpl(new HttpTransportMetricsImpl(), 1024, 0, null, null);
		bufferImpl.bind(input);
		return bufferImpl;
	}

	public static String[] readCRLFSeparatedBlock(SessionInputBuffer input) throws IOException {
		CharArrayBuffer line = new CharArrayBuffer(128);
		List<String> ret = new ArrayList<>();
		for (;;) {
			if (input.readLine(line) == -1) break;
			if (line.length() == 0) break;
			ret.add(line.toString());
			line.clear();
		}
		return ret.toArray(new String[ret.size()]);
	}

	public static HeaderGroup toHeaderGroup(String[] read) {
		HeaderGroup ret = new HeaderGroup();
		for (String s : read) {
			String[] p = s.split(":", 2);
			if (p.length != 2) continue;
			ret.addHeader(new BasicHeader(p[0].trim(), p[1].trim()));
		}
		return ret;
	}

	public static class WarcRecord {
		String[] warcHeaders;
		byte[] payload;
		Date date;
		UUID uuid;

		public WarcRecord(SessionInputBuffer buffer) throws IOException {
			this.warcHeaders = readCRLFSeparatedBlock(buffer);
			if (this.warcHeaders.length == 0) throw new EOFException();
			long contentLength = contentLength(this.warcHeaders);
			if (contentLength == -1) throw new WarcFormatException("Can't find Content-Length");
			final HeaderGroup hg = toHeaderGroup(this.warcHeaders);
			date = WarcHeader.parseDate(WarcHeader.getFirstHeader(hg, WarcHeader.Name.WARC_DATE).getValue());
			uuid = WarcHeader.parseId(WarcHeader.getFirstHeader(hg, WarcHeader.Name.WARC_RECORD_ID).getValue());
			this.payload = new byte[(int)contentLength];
			for (int read = 0; read < contentLength; read ++) this.payload[read] = (byte)buffer.read();
		}

		public WarcRecord(String path) throws IOException {
			this(warpSessionInputBuffer(new FileInputStream(path)));
		}

		public List<Set<String>> warcHeadersDiffs(it.unimi.di.law.warc.records.WarcRecord record) {
			return diff(toHeaderGroup(this.warcHeaders), record.getWarcHeaders());
		}

		public Date getDate() {
			return this.date;
		}

		public UUID getUUID() {
			return this.uuid;
		}

		@Override
		public String toString() {
			return Arrays.toString(warcHeaders);
		}
	}

	public static class InfoWarcRecord extends WarcRecord {
		String[] info;

		public InfoWarcRecord(SessionInputBuffer buffer) throws IOException {
			super(buffer);
			this.info = readCRLFSeparatedBlock(warpSessionInputBuffer(new ByteArrayInputStream(this.payload)));
		}

		public InfoWarcRecord(String path) throws IOException {
			this(warpSessionInputBuffer(new FileInputStream(path)));
		}

		public List<Set<String>> infoDiffs(it.unimi.di.law.warc.records.InfoWarcRecord record) {
			return diff(toHeaderGroup(this.info), record.getInfo());
		}

		@Override
		public String toString() {
			return "InputStreamMock Warcinfo: " + super.toString() + ", " + Arrays.toString(info);
		}
	}

	public static class HttpRequestWarcRecord extends WarcRecord {
		String[] headers;

		public HttpRequestWarcRecord(SessionInputBuffer buffer) throws IOException {
			super(buffer);
			this.headers = readCRLFSeparatedBlock(warpSessionInputBuffer(new ByteArrayInputStream(this.payload)));
		}

		public HttpRequestWarcRecord(String path) throws IOException {
			this(warpSessionInputBuffer(new FileInputStream(path)));
		}

		public List<Set<String>> headerDiffs(it.unimi.di.law.warc.records.HttpRequestWarcRecord record) {
			return diff(toHeaderGroup(this.headers), record.getAllHeaders());
		}

		@Override
		public String toString() {
			return "InputStreamMock Request: " + super.toString() + ", " + Arrays.toString(headers);
		}
	}

	public static class HttpResponseWarcRecord extends WarcRecord {
		String[] headers;
		byte[] entity;

		public HttpResponseWarcRecord(SessionInputBuffer buffer) throws IOException {
			super(buffer);
			buffer = warpSessionInputBuffer(new ByteArrayInputStream(this.payload));
			this.headers = readCRLFSeparatedBlock(buffer);
			long contentLength = contentLength(this.headers);
			InputStream blockStream = new ContentLengthInputStream(buffer, contentLength);
			this.entity = new byte[(int)contentLength];
			blockStream.read(this.entity);
			blockStream.close();
		}

		public HttpResponseWarcRecord(String path) throws IOException {
			this(warpSessionInputBuffer(new FileInputStream(path)));
		}

		public List<Set<String>> headerDiffs(it.unimi.di.law.warc.records.HttpResponseWarcRecord record) {
			return diff(toHeaderGroup(this.headers), record.getAllHeaders());
		}

		public boolean entityEquals(it.unimi.di.law.warc.records.HttpResponseWarcRecord record) throws IllegalStateException, IOException {
			byte[] otherEntity = ByteStreams.toByteArray(record.getEntity().getContent());
			return Arrays.equals(this.entity, otherEntity);
		}

		@Override
		public String toString() {
			return "InputStreamMock Response: " + super.toString() + ", " + Arrays.toString(headers) + ", \"" + (new String(entity)) + "\"";
		}
	}

}
