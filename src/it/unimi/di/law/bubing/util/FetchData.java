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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.frontier.ParsingThread;
import it.unimi.di.law.bubing.frontier.VisitState;
import it.unimi.di.law.bubing.parser.BinaryParser;
import it.unimi.di.law.bubing.test.NamedGraphServerHttpProxy;
import it.unimi.di.law.bubing.test.RandomNamedGraphServer;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.di.law.warc.util.InspectableCachedHttpEntity;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.security.NoSuchAlgorithmException;
import java.util.Queue;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.net.HttpHeaders;

//RELEASE-STATUS: DIST

/** Response of a HTTP request. At construction, data needed to issue requests are set up; in
 * particular, an {@link InspectableFileCachedInputStream} is instantiated that will be later used
 * to read data from the socket and to store them for the later stages (parsing, storing etc.): note
 * that the memory needed for the buffer of the {@link InspectableFileCachedInputStream}, as well as
 * the associated overflow file, are created at construction time, and will be disposed only by
 * calling {@link #close()}.
 *
 * <p>After construction, the {@link #fetch(URI, HttpClient, RequestConfig, VisitState, boolean)}
 * method is used to issue the request; after that, all data obtained as a {@linkplain #response() response} are available. All data is available until disposal, or until another
 * call to {@link #fetch(URI, HttpClient, RequestConfig, VisitState, boolean)}.
 *
 * <p>Note that since this object will be populated by one thread and used by another all fields
 * <strong>must</strong> be either <code>final</code>final or <code>volatile</code>. */

public class FetchData implements URIResponse, Closeable {
	private static final Logger LOGGER = LoggerFactory.getLogger(FetchData.class);

	// Material for fake HTTP responses (cuts out completely network access)
	private static final boolean FAKE = false;
	private static final RandomNamedGraphServer GRAPH_SERVER = FAKE ? new RandomNamedGraphServer(100000000, 50, 3) : null;
	private static final Header FAKE_CONTENT_TYPE = FAKE ? new BasicHeader(HttpHeaders.CONTENT_TYPE, "text/html") : null;
	private final HttpResponse FAKE_RESPONSE = FAKE ? new BasicHttpResponse(new ProtocolVersion("HTTP", 1, 1), 200, "OK") : null;
	{
		if (FAKE) FAKE_RESPONSE.setHeader(FAKE_CONTENT_TYPE);
	}

	/** The number of path elements for the hierarchical overflow files (see {@link it.unimi.di.law.bubing.util.Util#createHierarchicalTempFile(File, int, String, String)}). */
	public static final int OVERFLOW_FILES_RANDOM_PATH_ELEMENTS = 1;

	/** A future callback putting a {@link FetchData} in a result queue.
	 * {@link FetchData#exception} will be set to {@code null},
	 * to a specific exception or to {@link #CANCELLED} depending on whether
	 * the request {@linkplain #completed(Void) completed}, {@linkplain #failed(Exception) failed}
	 * or was {@linkplain #cancelled() cancelled}. */
	@SuppressWarnings("unused")
	private static final class EnqueueFetchedHttpResponseFutureCallback implements FutureCallback<Void> {
		/** A marker exception for {@linkplain FutureCallback#cancelled() cancelled} requests. */
		private final static Exception CANCELLED = new Exception();
		/** The {@link FetchData} that will be enqueued when the callback is invoked. */
		private final FetchData fetchData;
		/** The queue (to be set at each usage). */
		public volatile Queue<FetchData> results;

		public EnqueueFetchedHttpResponseFutureCallback(FetchData fetchData) {
			this.fetchData = fetchData;
		}

		private void common() {
			fetchData.endTime = System.currentTimeMillis();
			fetchData.httpGet.reset();
		}

		@Override
		public void completed(Void result) {
			common();
			results.add(fetchData);
		}

		@Override
		public void failed(Exception ex) {
			common();
			fetchData.exception = ex instanceof ClosedChannelException && fetchData.truncated ? null : ex;
			results.add(fetchData);
		}

		@Override
		public void cancelled() {
			common();
			fetchData.exception = CANCELLED;
			results.add(fetchData);
		}
	}


	/** The BUbiNG URL associated with this request. */
	protected volatile URI url;

	/** The visit state associated with this request. */
	public volatile VisitState visitState;

	/** The response from Apache Http Components returned during the last fetch. */
	protected volatile HttpResponse response;

	/** True if the last fetch was truncated because of exceedingly long response body. */
	protected volatile boolean truncated;

	/** {@link System#currentTimeMillis()} when the GET request was issued. */
	public volatile long startTime;

	/** {@link System#currentTimeMillis()} when the GET request was completed. */
	public volatile long endTime;

	/** The exception thrown in case of a failed fetch, or {@code null}. */
	public volatile Throwable exception;

	/** Whether we are fecthing a robots file. */
	public boolean robots;

	/** The wrapped entity used to replace with an {@link InspectableFileCachedInputStream} the content. */
	private final InspectableCachedHttpEntity wrappedEntity;

	/** The {@link InspectableFileCachedInputStream} used by {@link #wrappedEntity}. */
	private final InspectableFileCachedInputStream inspectableFileCachedInputStream;

	/** The content digest of the response. */
	private volatile byte[] digest;

	/** Tells whether this response is a duplicate. */
	private volatile boolean isDuplicate;

	/** The binary parser associated with this fetched response. */
	public final BinaryParser binaryParser;

	/** The {@link CachingAsyncByteConsumer} associated with this request. */
	//private final CachingAsyncByteConsumer cachingAsyncByteConsumer;

	/** The {@link EnumConstantNotPresentException} associated with this request. */
	//private final EnqueueFetchedHttpResponseFutureCallback enqueueFetchedHttpResponseFutureCallback;

	/** The request used by this response. */
	private final HttpGet httpGet;

	/** If true, this istance has been enqueued to the list of results and we are waiting
	 * for the signal of the {@link ParsingThread} that is analyzing it. */
	public volatile boolean inUse;

	/** The {@link RuntimeConfiguration}, cached. */
	private final RuntimeConfiguration rc;


	/** Creates a fetched response according to the given properties.
	 *
	 * @param rc the runtime configuration.
	 */
	public FetchData(final RuntimeConfiguration rc) throws NoSuchAlgorithmException, IllegalArgumentException, IOException {
		inspectableFileCachedInputStream = new InspectableFileCachedInputStream(rc.fetchDataBufferByteSize, it.unimi.di.law.bubing.util.Util.createHierarchicalTempFile(rc.responseCacheDir, OVERFLOW_FILES_RANDOM_PATH_ELEMENTS, getClass().getSimpleName() + "-", ".overflow"));
		this.rc = rc;
		wrappedEntity = new InspectableCachedHttpEntity(inspectableFileCachedInputStream);
		httpGet = new HttpGet();
		// TODO: This should be done more properly
		binaryParser = new BinaryParser(rc.digestAlgorithm);
		//context = new BasicHttpContext();
		//cachingAsyncByteConsumer = new CachingAsyncByteConsumer(this);
		//enqueueFetchedHttpResponseFutureCallback = new EnqueueFetchedHttpResponseFutureCallback(this);
	}

	/** Returns (an approximation of) the length of the response (headers and body).
	 *
	 * <p>We cannot compute easily the exact length because we must deduce it from headers and body length.
	 *
	 * @return (an approximation of) the length of the response content.
	 */
	public long length() {
		if (FAKE) return wrappedEntity.getContentLength() + "Content-Type: text/html\n\n".length();
		if (response == null) return 0;
		long length = 0;
		for(Header header: response.getAllHeaders()) length += header.getName().length() + header.getValue().length() + 1;
		return length + wrappedEntity.getContentLength();
	}

	/* Fetches a given URL.
	 *
	 * @param httpAsyncClient the asynchronous client that will be used to fetch {@code url}.
	 * @param url the URL to be used to populate this response.
	 * @param cookies cookies to be set when issuing the request, or {@code null} to set no cookies at all.
	 * @param visitState the {@link VisitState} associated with {@code url}.
	 * @param results a queue that will be used to enqueue this fetched response after a request is completed, or {@code null} for no enqueueing.
	 *
	 * @return a {@link Future} waiting for the page to be downloaded.
	 */
	/* public Future<Void> fetch(final HttpAsyncClient httpAsyncClient, final RequestConfig requestConfig, final URI url, final Cookie[] cookies, final VisitState visitState, final boolean robots, final Queue<FetchData> results) throws IOException {
		cookieStore.clear();
		if (! robots && cookies != null) for(Cookie cookie: cookies) cookieStore.addCookie(cookie);

		// check that all fields are cleared
		this.visitState = visitState;
		this.url = url;
		this.response = null;
		this.exception = null;
		this.truncated = false;
		this.isDuplicate = false;
		this.cookie = null;
		this.robots = robots;
		this.enqueueFetchedHttpResponseFutureCallback.results = results;

		assert url.getHost() != null : url;

		httpGet.reset();
		httpGet.setURI(url);
		if (requestConfig != null) httpGet.setConfig(requestConfig);
		wrappedEntity.clear(); // Reset backing file.
  		startTime = System.currentTimeMillis();

  		return httpAsyncClient.execute(HttpAsyncMethods.create(httpGet), cachingAsyncByteConsumer, context, results != null ? enqueueFetchedHttpResponseFutureCallback : null);
	}*/

	// TODO: PORTING: document
	/* (non-Javadoc)
	 * @see it.unimi.di.law.bubing.util.URIResponse#uri()
	 */
	@Override
	public URI uri() {
		return this.url;
	}

	// TODO: PORTING: document
	/* (non-Javadoc)
	 * @see it.unimi.di.law.bubing.util.URIResponse#response()
	 */
	@Override
	public HttpResponse response() {
		return this.response;
	}

	/** Fetches a given URL.
	 *
	 * @param httpClient the client that will be used to fetch {@code url}.
	 * @param url the URL to be used to populate this response.
	 * @param visitState the {@link VisitState} associated with {@code url}.
	 */
	public void fetch(final URI url, final HttpClient httpClient, final RequestConfig requestConfig, final VisitState visitState, final boolean robots) throws IOException {
		// ALERT: check that all fields are cleared.
		this.visitState = visitState;
		this.url = url;
		this.response = null;
		this.exception = null;
		this.truncated = false;
		this.isDuplicate = false;
		this.robots = robots;

		assert url.getHost() != null : url;

		httpGet.setURI(url);
		if (requestConfig != null) httpGet.setConfig(requestConfig);

		wrappedEntity.clear(); // Reset backing file.
 		startTime = System.currentTimeMillis();

 		if (FAKE) {
			final String content;
 			if (robots) content = "\n";
 			else {
 				CharSequence[] successors = GRAPH_SERVER.successors(url.toString());
 				// Note that this value must be kept in
	 			final StringBuilder builder = new StringBuilder(NamedGraphServerHttpProxy.estimateLength(successors));
 				NamedGraphServerHttpProxy.generate(url.hashCode(), builder, successors == null ? RandomNamedGraphServer.EMPTY_CHARSEQUENCE_ARRAY : successors, false);
 				content = builder.toString();
 			}
 			final BasicHttpEntity fakeEntity = new BasicHttpEntity();
			fakeEntity.setContent(IOUtils.toInputStream(content, Charsets.ISO_8859_1));
 			fakeEntity.setContentLength(content.length());
 			fakeEntity.setContentType(FAKE_CONTENT_TYPE);
 			wrappedEntity.setEntity(fakeEntity);
 			wrappedEntity.copyContent(rc.responseBodyMaxByteSize, startTime, rc.connectionTimeout, 10);
			(response = FAKE_RESPONSE).setEntity(wrappedEntity);
 		}
 		else {
 			try {
				final URI uri = httpGet.getURI();
				final String scheme = uri.getScheme();
				final int port = uri.getPort() == -1 ? (scheme.equals("https") ? 443 : 80) : uri.getPort();
 				final HttpHost httpHost = visitState != null ? 
					new HttpHost(InetAddress.getByAddress(visitState.workbenchEntry.ipAddress), uri.getHost(), port, scheme) :
 					new HttpHost(uri.getHost(), port, scheme);
 				httpClient.execute(httpHost, httpGet, new ResponseHandler<Void>() {

 					@Override
 					public Void handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
 						FetchData.this.response = response;
 						final HttpEntity entity = response.getEntity();

 						if (entity == null) LOGGER.warn("Null entity for URL " + url);
 						else {
 							wrappedEntity.setEntity(entity);
 							truncated = wrappedEntity.copyContent(rc.responseBodyMaxByteSize, startTime, rc.connectionTimeout, 10);
 							if (truncated) httpGet.abort();
 						}
 						return null;
 					}});

 				response.setEntity(wrappedEntity);
 			}
 			catch(IOException e) {
 				exception = e instanceof ClientProtocolException ? e.getCause() : e;
 			}
 		}

		endTime = Math.max(System.currentTimeMillis(), startTime); // Work around non-monotonicity of System.currentTimeMillis()
		httpGet.reset(); // Release resources.
	}

	/**
	 * Set the digest with a given value
	 *
	 * @param digest the value to be set for <code>digest</code>
	 */
	public void digest(byte[] digest) {
		this.digest = digest;
	}

	/**
	 * Get the digest
	 *
	 * @return the digest of this <code>FetchData</code>
	 */
	public byte[] digest() {
		return digest;
	}

	/**
	 * Mark the current <code>FetchData</code> as duplicated or not duplicated
	 *
	 * @param isDuplicate a boolean value indicating whether the current <code>FetchData</code> should be marked as duplicated or not
	 */
	public void isDuplicate(boolean isDuplicate) {
		this.isDuplicate = isDuplicate;
	}

	/**
	 * Get whether the current <code>FetchData</code> is duplicate or not
	 *
	 * @return <code>true</code> if this is duplicate or not
	 */
	public boolean isDuplicate() {
		return isDuplicate;
	}

	@Override
	public String toString() {
		return "[" + url + " (" + response.getStatusLine() + ")]";
	}

	/** Invokes {@link HttpGet#abort()} on the underlying request. */
	public void abort() {
		httpGet.abort();
	}

	/** {@linkplain InspectableFileCachedInputStream#dispose() Disposes} the underlying {@link InspectableFileCachedInputStream}. */
	@Override
	public void close() throws IOException {
		inspectableFileCachedInputStream.dispose();
	}

	/*
	public static void main(String[] arg) throws Exception {
		if (arg.length < 2) {
			System.err.println("Args: CONFIG URL ... ");
			System.exit(1);
		}

		final BaseConfiguration configuration = new BaseConfiguration();
		configuration.addProperty("name", "BUbiNG");
		configuration.addProperty("group", "test");
		configuration.addProperty("weight", "1");

		DefaultHttpAsyncClient httpAsyncClient = new DefaultHttpAsyncClient();
		httpAsyncClient.start();
		ArrayBlockingQueue<FetchedHttpResponse> results = new ArrayBlockingQueue<FetchedHttpResponse>(10);

		for (int i = 1; i < arg.length; i++) {
			FetchedHttpResponse resp = new FetchedHttpResponse(new RuntimeConfiguration(new StartupConfiguration(arg[0], configuration)));
			resp.fetch(httpAsyncClient, BURL.parse(arg[i]), null, null, false, results);
		}

		for (int i = 1; i < arg.length; i++) {
			FetchedHttpResponse resp = results.take();
			System.out.println("-------------------");
			System.out.println("*** URL: " + resp.uri());
			System.out.println("*** Status line: " + resp.get());
			System.out.println("*** Status: " + resp.status());
			System.out.println("*** Headers: " + resp.headers());
			System.out.println("*** Content-length: " + resp.headers().get(HttpHeaders.CONTENT_LENGTH));
			System.out.println("*** Cookies: " + Arrays.toString(resp.cookie));
			InputStream is = resp.contentAsStream();
			System.out.println();
			BufferedReader r = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = r.readLine()) != null) System.out.println(line);
		}

		httpAsyncClient.shutdown();
	}
	*/
}
