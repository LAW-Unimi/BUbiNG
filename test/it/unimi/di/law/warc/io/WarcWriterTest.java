package it.unimi.di.law.warc.io;

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

import static org.junit.Assert.assertEquals;
import it.unimi.di.law.TestUtil;
import it.unimi.di.law.warc.records.HttpRequestWarcRecord;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.InfoWarcRecord;
import it.unimi.di.law.warc.records.InputStreamTestMocks;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.BufferedHttpEntityFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.junit.Test;

public class WarcWriterTest {

	private static final String WARCINFO_PATH = TestUtil.getTestFile(WarcReader.class, "warcinfo.warc", false);
	private static final String REQUEST_PATH = TestUtil.getTestFile(WarcReader.class, "request.warc", false);
	private static final String RESPONSE_PATH = TestUtil.getTestFile(WarcReader.class, "response.warc", false);

	@SuppressWarnings("resource")
	@Test
	public void testSingleInfo() throws IOException {
		FileOutputStream fos = new FileOutputStream("/tmp/warcinfo.warc");
		new UncompressedWarcWriter(fos).write(new UncompressedWarcReader(new FileInputStream(WARCINFO_PATH)).read());
		fos.close();
		InfoWarcRecord actual =(InfoWarcRecord) new UncompressedWarcReader(new FileInputStream(WARCINFO_PATH)).read();
		InputStreamTestMocks.InfoWarcRecord expected = new InputStreamTestMocks.InfoWarcRecord("/tmp/warcinfo.warc");
		assertEquals(InputStreamTestMocks.EMPTY_DIFFS, expected.warcHeadersDiffs(actual));
		assertEquals(InputStreamTestMocks.EMPTY_DIFFS, expected.infoDiffs(actual));
	}

	@SuppressWarnings("resource")
	@Test
	public void testSingleRequest() throws IOException, SecurityException, IllegalArgumentException {
		FileOutputStream fos = new FileOutputStream("/tmp/request.warc");
		new UncompressedWarcWriter(fos).write(new UncompressedWarcReader(new FileInputStream(REQUEST_PATH)).read());
		fos.close();
		HttpRequestWarcRecord actual = (HttpRequestWarcRecord) new UncompressedWarcReader(new FileInputStream(REQUEST_PATH)).read();
		InputStreamTestMocks.HttpRequestWarcRecord expected = new InputStreamTestMocks.HttpRequestWarcRecord("/tmp/request.warc");
		assertEquals(InputStreamTestMocks.EMPTY_DIFFS, expected.warcHeadersDiffs(actual));
		assertEquals(InputStreamTestMocks.EMPTY_DIFFS, expected.headerDiffs(actual));
	}

	@SuppressWarnings("resource")
	@Test
	public void testSingleResponse() throws IOException, SecurityException, IllegalArgumentException {
		FileOutputStream fos = new FileOutputStream("/tmp/response.warc");
		new UncompressedWarcWriter(fos).write(new UncompressedWarcReader(new FileInputStream(RESPONSE_PATH)).read());
		fos.close();
		HttpResponseWarcRecord actual = (HttpResponseWarcRecord) new UncompressedWarcReader(new FileInputStream(RESPONSE_PATH)).read();
		InputStreamTestMocks.HttpResponseWarcRecord expected = new InputStreamTestMocks.HttpResponseWarcRecord("/tmp/response.warc");
		assertEquals(InputStreamTestMocks.EMPTY_DIFFS, expected.warcHeadersDiffs(actual));
		assertEquals(InputStreamTestMocks.EMPTY_DIFFS, expected.headerDiffs(actual));
	}

	@SuppressWarnings("resource")
	@Test
	public void testMisc() throws IOException, WarcFormatException, InterruptedException {
		InfoWarcRecord info = (InfoWarcRecord) new UncompressedWarcReader(new FileInputStream(WARCINFO_PATH)).read();
		HttpRequestWarcRecord request = (HttpRequestWarcRecord) new UncompressedWarcReader(new FileInputStream(REQUEST_PATH)).read();
		URI fakeUri = null;
		try {
			fakeUri = new URI("http://this.is/a/fake");
		} catch (URISyntaxException ignored) {}
		HttpResponseWarcRecord response = new HttpResponseWarcRecord(fakeUri, (HttpResponse) new UncompressedWarcReader(new FileInputStream(RESPONSE_PATH)).read(), BufferedHttpEntityFactory.INSTANCE);

		FileOutputStream fos = new FileOutputStream("/tmp/misc.warc");
		WarcWriter ww = new UncompressedWarcWriter(fos);
		ww.write(info);
		ww.write(request);
		ww.write(response);
		ww.write(request);
		ww.write(response);
		ww.write(request);
		ww.write(response);
		fos.close();

		WarcReader wr = new UncompressedWarcReader(new FileInputStream("/tmp/misc.warc"));
		List<WarcRecord> actual = new ArrayList<>();
		for (;;) {
			WarcRecord ret = wr.read();
			if (ret == null) break;
			actual.add(ret);
		}
		WarcReaderTest.assertMockEquals("/tmp/misc.warc", actual);
	}

}
