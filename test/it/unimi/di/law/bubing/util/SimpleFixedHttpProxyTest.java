package it.unimi.di.law.bubing.util;

/*
 * Copyright (C) 2012-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Test;

import com.google.common.base.Charsets;

public class SimpleFixedHttpProxyTest {

	SimpleFixedHttpProxy proxy;

	@After
	public void tearDownProxy() throws InterruptedException, IOException {
		if (proxy != null) proxy.stopService();
	}

	@Test
	public void testWithProxy() throws IOException {
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

		HttpClient httpClient = HttpClients.createDefault();
		RequestConfig requestConfig = RequestConfig.custom()
				.setRedirectsEnabled(false)
				.setProxy(new HttpHost("localhost", proxy.port()))
				.build();

		HttpGet request0 = new HttpGet(url0);
		request0.setConfig(requestConfig);
		httpClient.execute(request0, new ResponseHandler<Void>() {
			@Override
			public Void handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				assertEquals(content0, IOUtils.toString(response.getEntity().getContent(), Charsets.ISO_8859_1));
				return null;
			}});

		HttpGet request1 = new HttpGet(url1);
		request1.setConfig(requestConfig);
		httpClient.execute(request1, new ResponseHandler<Void>() {
			@Override
			public Void handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				assertEquals(url0.toString(), response.getFirstHeader(HttpHeaders.LOCATION).getValue());
				assertEquals(content1, IOUtils.toString(response.getEntity().getContent(), Charsets.ISO_8859_1));
				return null;
			}});


	}

	@Test
	public void testWithCookies() throws IOException {
		final URI url0 = BURL.parse(new MutableString("http://foo.bar/goo/first.html"));
		final URI url1 = BURL.parse(new MutableString("http://foo.bar/goo/second.html"));
		final String content0 = "";
		final String content1 = "";

		proxy = new SimpleFixedHttpProxy(true);

		proxy.add200(url0, "Set-cookie: cookie=hello; Domain: foo.bar", content0);
		proxy.add200(url1, "", content1);

		proxy.start();

		HttpClient onePageClient = HttpClients.createDefault();
		HttpClient twoPageClient = HttpClients.createDefault();
		RequestConfig requestConfig = RequestConfig.custom()
				.setRedirectsEnabled(false)
				.setProxy(new HttpHost("localhost", proxy.port()))
				.build();

		// The two-page client has a cookie set by url0, and shows it in url1
		HttpGet request0 = new HttpGet(url0);
		request0.setConfig(requestConfig);
		twoPageClient.execute(request0, new ResponseHandler<Void>() {
			@Override
			public Void handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				assertEquals(content0, IOUtils.toString(response.getEntity().getContent(), Charsets.ISO_8859_1));
				return null;
			}});

		HttpGet request1 = new HttpGet(url1);
		request1.setConfig(requestConfig);
		twoPageClient.execute(request1, new ResponseHandler<Void>() {
			@Override
			public Void handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				assertEquals(content1 + "Cookie: cookie=hello", IOUtils.toString(response.getEntity().getContent(), Charsets.ISO_8859_1));
				return null;
			}});
		// The one-page client did not visit url0, so it does not show any cookie to url1
		onePageClient.execute(request1, new ResponseHandler<Void>() {
			@Override
			public Void handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
				assertEquals(content1, IOUtils.toString(response.getEntity().getContent(), Charsets.ISO_8859_1));
				return null;
			}});

	}
}
