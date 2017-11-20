package it.unimi.di.law.bubing.util;

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

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.frontier.FetchingThread;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;

public class FetchDataTest {
	SimpleFixedHttpProxy proxy;
	private RuntimeConfiguration testConfiguration;

	public static CloseableHttpClient getHttpClient(final HttpHost proxy, final boolean redirects) {
		return getHttpClient(proxy, redirects, new BasicCookieStore());
	}

	public static CloseableHttpClient getHttpClient(final HttpHost proxy, final boolean redirects, final CookieStore cookieStore) {
		final Builder builder = RequestConfig.custom()
				.setRedirectsEnabled(redirects)
				.setMaxRedirects(5);
		if (proxy != null) builder.setProxy(proxy);
		final RequestConfig requestConfig = builder.build();
		return HttpClients.custom()
				.setDefaultRequestConfig(requestConfig)
				.setDefaultCookieStore(cookieStore)
				.setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
				.build();
	}

	@Before
	public void setupTestConfiguration() throws ConfigurationException, IOException, IllegalArgumentException, ClassNotFoundException {
		testConfiguration = Helpers.getTestConfiguration(this);
		System.err.println(testConfiguration.rootDir);
	}

	@After
	public void tearDownProxy() throws InterruptedException, IOException {
		if (proxy != null) proxy.stopService();
	}

	@Test
	public void testSyncWithProxy() throws IOException, NoSuchAlgorithmException, IllegalArgumentException, ConfigurationException, ClassNotFoundException {
		final URI url0 = BURL.parse(new MutableString("http://foo.bar/goo/zoo.html"));
		final URI url1 = BURL.parse(new MutableString("http://foo.bar/goo/naa.html"));
		final String content0 = "Esempio di pagina html...";
		final String content1 = "<html><head>\n<title>Moved</title>\n</head>\n<body>\n<h1>Moved</h1>\n<p>This page has moved to <a href=\"http://foo.bar/goo/zoo.html\">http://foo.bar/goo/zoo.html</a>.</p>\n</body>\n</html>";

		proxy = new SimpleFixedHttpProxy();
		proxy.add200(url0, "", content0);
		proxy.addNon200(url1,
				"HTTP/1.1 301 Moved Permanently\n" +
						"Location: http://foo.bar/goo/zoo.html\n" +
						"Content-Type: text/html\n",
						content1
				);

		proxy.start();

		HttpClient httpClient = getHttpClient(new HttpHost("localhost", proxy.port()), false);

		// Test normal operations
		FetchData fetchData = new FetchData(testConfiguration);

		fetchData.fetch(url0, httpClient, null, null, false);
		assertNull(fetchData.exception);
		assertEquals(content0, IOUtils.toString(fetchData.response().getEntity().getContent(), Charsets.ISO_8859_1));
		assertEquals(content0, IOUtils.toString(fetchData.response().getEntity().getContent(), Charsets.ISO_8859_1));

		fetchData.fetch(url1, httpClient, null, null, false);
		assertNull(fetchData.exception);
		assertEquals(url0.toString(), fetchData.response().getFirstHeader(HttpHeaders.LOCATION).getValue());
		assertEquals(content1, IOUtils.toString(fetchData.response().getEntity().getContent(), Charsets.ISO_8859_1));
		assertEquals(content1, IOUtils.toString(fetchData.response().getEntity().getContent(), Charsets.ISO_8859_1));

		fetchData.close();

		// Test for truncated response body
		for(int l = 0; l < 2000; l = l * 2 + 1) {
			BaseConfiguration baseConfiguration = new BaseConfiguration();
			baseConfiguration.setProperty("responseBodyMaxByteSize", Integer.toString(l));
			fetchData = new FetchData(Helpers.getTestConfiguration(this, baseConfiguration, true));

			fetchData.fetch(url1, httpClient, null, null, false);
			assertNull(fetchData.exception);
			assertEquals(content1.substring(0, Math.min(l, content1.length())), IOUtils.toString(fetchData.response().getEntity().getContent(), Charsets.ISO_8859_1));
			fetchData.close();
		}
	}


	@Test
	public void testSyncWithCookies() throws IOException, NoSuchAlgorithmException, IllegalArgumentException {
		final URI url0 = BURL.parse(new MutableString("http://foo.bar/goo/first.html"));
		final URI url1 = BURL.parse(new MutableString("http://foo.bar/goo/second.html"));
		final String content0 = "";
		final String content1 = "";

		proxy = new SimpleFixedHttpProxy(true);

		proxy.add200(url0, "Set-cookie: cookie=hello; Domain: foo.bar", content0);
		proxy.add200(url1, "", content1);

		proxy.start();

		FetchData fetchData = new FetchData(testConfiguration);

		BasicCookieStore cookieStore = new BasicCookieStore();
		HttpClient httpClient = getHttpClient(new HttpHost("localhost", proxy.port()), false, cookieStore);

		// This shows we do receive cookies
		fetchData.fetch(url0, httpClient, null, null, false);
		Cookie[] cookie = FetchingThread.getCookies(url0, cookieStore, testConfiguration.cookieMaxByteSize);
		assertEquals("cookie", cookie[0].getName());
		assertEquals("hello", cookie[0].getValue());

		// This shows we do send cookies
		// First attempt: sending back the same cookies just received
		fetchData.fetch(url1, httpClient, null, null, false);
		assertTrue(IOUtils.toString(fetchData.response().getEntity().getContent(), Charsets.ISO_8859_1).contains("hello"));
		// Second attempt: sending new cookies
		cookieStore.clear();
		BasicClientCookie clientCookie = new BasicClientCookie("returned", "cookie");
		clientCookie.setDomain("foo.bar");
		cookieStore.addCookie(clientCookie);
		fetchData.fetch(url1, httpClient, null, null, false);
		assertTrue(IOUtils.toString(fetchData.response().getEntity().getContent(), Charsets.ISO_8859_1).contains("returned"));

		// This shows we do not send undue cookies
		cookieStore.clear();
		clientCookie.setDomain("another.domain");
		cookieStore.addCookie(clientCookie);
		fetchData.fetch(url1, httpClient, null, null, false);
		assertFalse(IOUtils.toString(fetchData.response().getEntity().getContent(), Charsets.ISO_8859_1).contains("returned"));

		System.out.println(Arrays.toString(FetchingThread.getCookies(url1, cookieStore, testConfiguration.cookieMaxByteSize)));

		fetchData.close();
	}
}
