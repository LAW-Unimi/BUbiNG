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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.cookie.Cookie;
import org.apache.http.message.BasicStatusLine;

/** Mock implementations for tests. */

public class MockFetchedResponses {

	/** A response that is configured with a {@link it.unimi.di.law.bubing.RuntimeConfiguration} and produces, at
	 *  fetch, a random HTML content, containing the given URL as title, and containing a number of links some
	 *  of them internal (fake), and some of them external (fake).
	 */
	public static class RandomFetchedHttpResponse extends FetchData {
		private InspectableFileCachedInputStream inspectableBufferedInputStream;
		private final StatusLine STATUS_LINE = new BasicStatusLine(new ProtocolVersion("HTTP", 0, 1), 200, "OK");

		public RandomFetchedHttpResponse(RuntimeConfiguration conf) throws IOException, NoSuchAlgorithmException, IllegalArgumentException {
			super(conf);
			inspectableBufferedInputStream = new InspectableFileCachedInputStream();
		}


		public InspectableFileCachedInputStream contentAsStream() throws IOException {
			inspectableBufferedInputStream.position(0);
			return inspectableBufferedInputStream;
		}

		public void fetch(URI uri) throws IOException {
			this.url = uri;
			MutableString s = new MutableString();
			s.append("<html><head><title>").append(uri).append("</title></head>\n");
			s.append("<body>\n");

			try {
				final int host = Integer.parseInt(uri.getHost());
				final int page = Integer.parseInt(uri.getRawPath().substring(1));
				final Random random = new Random(host << 32 | page);
				for(int i = 0; i < 10; i++)
					s.append("<a href=\"http://").append(host).append('/').append(random.nextInt(10000)).append("\">Link ").append(i).append("</a>\n");
				s.append("<a href=\"http://").append(random.nextInt(1000)).append('/').append(random.nextInt(10000)).append("\">External link ").append("</a>\n");

			}
			catch(NumberFormatException e) {}
			s.append("</body></html>\n");
			inspectableBufferedInputStream.write(ByteBuffer.wrap(Util.toByteArray(s.toString())));
		}

		public Map<String, String> headers() { return Object2ObjectMaps.singleton("content-type", "text/html"); }
		public StatusLine statusLine() { return STATUS_LINE; }
		@Override
		public URI uri() { return url; }
		public List<Cookie> cookies() { return Collections.emptyList(); }
		public void clearCookies() {}
		public void dispose() {}
	}
}
