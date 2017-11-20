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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.lang.MutableString;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/** An HTTP proxy that only serves a given set of URIs, having an associated set of (hardwired) responses. Some responses
 *  are {@linkplain #add200(URI, String, String) standard}: they provide automatically the status line (200) and some headers(connection, content-type and content-length),
 *  and some extra headers can be provided. {@linkplain #addNon200(URI, String, String) Other responses} are fully provided (in this case only the content-length field
 *  is automatically added). If {@link #returnCookies} is set to <code>true</code> in the constructor,
 *  the cookies header is returned <b>at the end</b> of the request content (only the first cookie header line in the
 *  response is actually returned) and the very same cookie is also returned in the headers, with a value
 *  prefixed by an x. */

public class SimpleFixedHttpProxy extends Thread {

	private final static Logger LOGGER = LoggerFactory.getLogger(SimpleFixedHttpProxy.class);

	public final boolean returnCookies;

	private final Object2ObjectMap<URI, String> uri2Headers = new Object2ObjectOpenHashMap<>();
	private final Object2ObjectMap<URI, String> uri2Content = new Object2ObjectOpenHashMap<>();

	private final ServerSocket serverSocket;

	/** Creates a proxy on a specific port.
	 *
	 * @param port the port.
	 * @param returnCookies if <code>true</code>, the cookies provided in the request are returned at the end of the content.
	 */
	private SimpleFixedHttpProxy(final int port, boolean returnCookies) throws IOException {
		this.serverSocket = new ServerSocket(port);
		this.returnCookies = returnCookies;
	}

	/** Creates a proxy on the port specified in the given configuration.
	 *
	 * @param conf the configuration.
	 * @param returnCookies if <code>true</code>, the cookies provided in the request are returned at the end of the content.
	 */
	public SimpleFixedHttpProxy(final RuntimeConfiguration conf, final boolean returnCookies) throws IOException {
		this(conf.proxyPort, returnCookies);
	}

	/** Creates a proxy on some available port.
	 *
	 * @param returnCookies if <code>true</code>, the cookies provided in the request are returned at the end of the content.
	 */
	public SimpleFixedHttpProxy(final boolean returnCookies) throws IOException {
		this(0, returnCookies);
	}

	/** Creates a proxy on some available port.
	 * @throws IOException
	 */
	public SimpleFixedHttpProxy() throws IOException {
		this(false);
	}


	/** Returns the port this proxy is listening to.
	 *
	 * @return the port this proxy is listening to.
	 */
	public int port() {
		return serverSocket.getLocalPort();
	}

	/** Adds a 200-status response. Besides the status line, the response is associated
	 *  with the following standard headers: connection (close), content-type (text/html, ISO-8859-1),
	 *  content-length (specifying the length).
	 *
	 * @param uri the URI this response is associated with.
	 * @param extraHeaders the extra headers.
	 * @param content the content.
	 */
	public void add200(final URI uri, final String extraHeaders, final String content) {
		uri2Headers.put(uri, "HTTP/1.1 200 OK\n" +
				"Connection: close\n" +
				"Content-Type: text/html; charset=iso-8859-1\n" +
				extraHeaders);
		uri2Content.put(uri, content);

	}

	/** Adds a generic response. The response is associated
	 *  with only the connection (close) and content-length header. The status line and all other headers must be explicitly passed
	 *  to the method.
	 *
	 * @param uri the URI this response is associated with.
	 * @param statusLineAndHeaders the status line and headers.
	 * @param content the content.
	 */
	public void addNon200(final URI uri, final String statusLineAndHeaders, final String content) {
		uri2Headers.put(uri,
				statusLineAndHeaders +
				"Connection: close\n");
		uri2Content.put(uri, content);

	}

	@Override
	public void run() {
		try {
			for (;;) {
				if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting for a request");
				if (serverSocket.isClosed()) {
					if (LOGGER.isDebugEnabled()) LOGGER.debug("The server socket is closed --- exiting");
					return;
				}
				Socket socket = serverSocket.accept();
				if (isInterrupted()) {
					socket.close();
					return;
				}
				if (LOGGER.isDebugEnabled()) LOGGER.debug("Acceping a request");
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				if (isInterrupted()) {
					socket.close();
					br.close();
					return;
				}
				String line = br.readLine();

				if (isInterrupted()) {
					socket.close();
					br.close();
					return;
				}
				if (LOGGER.isDebugEnabled()) LOGGER.debug("Received request: " + line);

				int first, second;

				first = line.indexOf(' ');
				second = first >= 0? line.indexOf(' ', first + 1) : -1;

				final PrintWriter out = new PrintWriter(new OutputStreamWriter(new FastBufferedOutputStream(socket.getOutputStream())));
				if (first < 0 || second < 0) {
					out.println("HTTP/1.1 500 Server error");
					out.println();
					out.close();
				}
				else {
					URI uri = BURL.parse(new MutableString(line.substring(first + 1, second)));
					String cookieLine = "";
					while ((line = br.readLine()).length() > 0) {
						if (line.toLowerCase().startsWith("cookie:")) {
							cookieLine = line;
							break;
						}
					}

					if (uri2Headers.containsKey(uri)) {
						String actualContent = uri2Content.get(uri) + (returnCookies? cookieLine : "");
						String connection = uri2Headers.get(uri) + "Content-length: " + actualContent.length() + "\n\n" + actualContent;
						if (LOGGER.isDebugEnabled()) LOGGER.debug("Going to answer " + connection);
						out.print(connection);
					}
					else {
						out.println("HTTP/1.1 404 Not found");
						out.println("Connection: close");
						out.println("Content-Type: text/html; charset=iso-8859-1");
						out.println();
						out.println("<html>");
						out.println("<head></head>");
						out.println("<body>");
						out.println("<h1>404 Not found</h1>");
						out.println("</body>");
						out.println("</html>");
					}
					out.close();
				}
				br.close();
				socket.close();
			}
		}
		catch (SocketException e) {
			if (serverSocket.isClosed()) {
				if (LOGGER.isDebugEnabled()) LOGGER.debug("Exiting proxy");
				return; // Not really an exception: serverSocket was closed to interrupt the proxy
			}
			throw new RuntimeException(e);
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public void stopService() throws InterruptedException, IOException {
		interrupt();
		if (serverSocket != null) serverSocket.close();
		join();
	}

	@Override
	public void finalize() throws IOException {
		if (serverSocket != null) serverSocket.close();
	}

	@Override
	public String toString() {
		return uri2Headers.toString() + uri2Content.toString();
	}

}
