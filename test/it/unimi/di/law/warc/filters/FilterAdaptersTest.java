package it.unimi.di.law.warc.filters;

/*
 * Copyright (C) 2008-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.warc.filters.parser.FilterParser;
import it.unimi.di.law.warc.filters.parser.ParseException;

import java.net.URI;

import org.junit.Test;

//RELEASE-STATUS: DIST

public class FilterAdaptersTest {

	@Test
	public void testAdaptationSimple() throws ParseException {
		FilterParser<URI> filterParser = new FilterParser<>(URI.class);
		Filter<URI> filter = filterParser.parse("HostEquals(www.dsi.unimi.it) or " +
				"it.unimi.di.law.warc.filters.FiltersTest$StartsWithStringFilter(http://xx)");
		System.out.println("TESTING: " + filter);
		assertTrue(filter.apply(BURL.parse("http://www.dsi.unimi.it/mb")));
		assertTrue(filter.apply(BURL.parse("http://xxx.foo.bar")));
		assertFalse(filter.apply(BURL.parse("http://yyy.foo.bar")));
	}

	// TODO: PORTING: see how to replace 'it.unimi.di.law.warc.util.HttpComponentsHttpResponse'
	/*
	@Test
	public void testAdaptation() throws ParseException, IOException {
		if (Helpers.networkAccessDenied()) return;
		final FilterParser<HttpResponse> filterParser = new FilterParser<HttpResponse>(HttpResponse.class);
		final Filter<HttpResponse> filter = filterParser.parse("HostEquals(www.dsi.unimi.it) or StatusCategory(4)");
		System.out.println("TESTING: " + filter);

		HttpClient httpClient = new DefaultHttpClient();
		httpClient.getParams().setBooleanParameter(ClientPNames.HANDLE_REDIRECTS, false);
		HttpComponentsHttpResponse httpComponentsHttpResponse = new HttpComponentsHttpResponse();

		URI url = URI.create("http://www.dsi.unimi.it/boldi");
		httpComponentsHttpResponse.set(url, httpClient.execute(new HttpGet(url)));
		assertTrue(filter.apply(httpComponentsHttpResponse)); // ACCEPT because host is correct
		httpComponentsHttpResponse.consume();

		url = URI.create("http://www.dsi.unimi.it/foobars");
		httpComponentsHttpResponse.set(url, httpClient.execute(new HttpGet(url)));
		assertTrue(filter.apply(httpComponentsHttpResponse)); // ACCEPT because status category correct
		httpComponentsHttpResponse.consume();

		url = URI.create("http://boldi.dsi.unimi.it/");
		httpComponentsHttpResponse.set(url, httpClient.execute(new HttpGet(url)));
		assertFalse(filter.apply(httpComponentsHttpResponse)); // REJECT
		httpComponentsHttpResponse.consume();

		httpClient.getConnectionManager().shutdown();
	}
	*/

}
