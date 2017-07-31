package it.unimi.di.law.warc.filters;

/*
 * Copyright (C) 2008-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
