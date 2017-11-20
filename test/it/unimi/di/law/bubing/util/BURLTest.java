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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.net.URI;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/** A class to test {@link BURL}. */

public class BURLTest {

	public static final Logger LOGGER = LoggerFactory.getLogger(BURLTest.class);

	@Test
	public void testEndingNull() {
		assertNull(BURL.parse("a%00"));
		assertNull(BURL.parse("htt%00p://a"));
		assertNull(BURL.parse("http://a%00b/"));
		assertNull(BURL.parse("http://a/%00b"));
		assertNull(BURL.parse("http://a/b?%00b"));
	}

	@Test
	public void testControlChars() {
		assertNull(BURL.parse("http://a b/"));
		assertEquals("http://a/%20b/", BURL.parse("http://a/ b/").toString());
		assertEquals("http://a/b%20c/", BURL.parse("http://a/b c/").toString());
		assertEquals("http://a/b?%20c", BURL.parse("http://a/b?%20c").toString());
		assertEquals("http://a/%09b/", BURL.parse("http://a/\tb/").toString());
		assertEquals("http://a/b/", BURL.parse("http:\\\\a\\b\\").toString());
		assertNull(BURL.parse("http://a/\nb/"));
		assertNull(BURL.parse("http://a/\rb/"));
		assertNull(BURL.parse("http://a\tb/"));

		//+ is allowed in the path part, but it has to be encoded in the query part
		assertEquals("http://a/b+c/", BURL.parse("http://a/b+c/").toString());
		// + is encoded just for compatibility--we don't do it
		//assertEquals("http://a/b+c?d+e", BURL.parse("http://a/b+c?d%2Be").toString());

		//"?" is allowed unescaped anywhere within a query part,
		assertEquals("http://a/b?c?d", BURL.parse("http://a/b?c?d").toString());

		//"/" is allowed unescaped anywhere within a query part,
		assertEquals("http://a/b?c/d", BURL.parse("http://a/b?c/d").toString());

		//"=" is allowed unescaped anywhere within a path parameter or query parameter value, and within a path segment,
		assertEquals("http://a/b=b;b=b=b/c?c=d", BURL.parse("http://a/b=b;b=b=b/c?c=d").toString());

		//":@-._~!$&'()*+,;=" are allowed unescaped anywhere within a path segment part,
		assertEquals("http://a/b;c/", BURL.parse("http://a/b;c/").toString());
		assertEquals("http://a/b;c=d;d=e;=/", BURL.parse("http://a/b;c=d;d=e;=/").toString());

		//Hence:
		// + is encoded just for compatibility--we don't do it
		// assertEquals("http://example.com/blue+light%20blue?blue%2Blight+blue" , BURL.parse("http://example.com/blue+light blue?blue+light blue").toString());
		//The following is a valid URL
		assertEquals("http://example.com/:@-._~!$&'()*+,=;:@-._~!$&'()*+,=:@-._~!$&'()*+,==?/?:@-._~!$'()*+,;=/?:@-._~!$'()*+,;==", BURL.parse("http://example.com/:@-._~!$&'()*+,=;:@-._~!$&'()*+,=:@-._~!$&'()*+,==?/?:@-._~!$'()*+,;=/?:@-._~!$'()*+,;==#/?:@-._~!$&'()*+,;=").toString());
	}

	@Test
	public void testUTF8() {
		assertNull(BURL.parse("http://a/\u00A0b/"));
		assertEquals("/Top/World/Espa%C3%B1ol/", BURL.parse("/Top/World/Espa\u00F1ol/").toString());
		assertEquals("http://foo/Top/World/Espa%C3%B1ol/", BURL.parse("http://foo/Top/World/Espa\u00F1ol/").toString());
		assertEquals("/Top/World/Espa%C3%B1ol/", BURL.parse("http://foo/Top/World/Espa\u00F1ol/").getRawPath());
	}

	@Test
	public void testTrailingSlash() {
		assertEquals("http://a.b/", BURL.parse("http://a.b").toString());
		assertEquals("", BURL.parse("#frag").toString());
	}

	@Test
	public void testNormalisation() {
		assertEquals("http://a.b/a", BURL.parse("http://a.b/c/../a").toString());
		assertEquals("a", BURL.parse("./a").toString());
		assertEquals("http://a/", BURL.parse("HTTP://A/").toString());
		assertEquals("http://a/", BURL.parse("HTTP://A/").toString());
		assertEquals("http://a/B", BURL.parse("HTTP://A/B").toString());

	}

	@Test
	public void testFromNormalizedURI() {
		assertFalse(BURL.parse("http://a.b/c/../a").equals(BURL.fromNormalizedByteArray("http://a.b/c/../a".getBytes(Charsets.US_ASCII))));
		assertEquals(BURL.parse("http://a.b/a"), BURL.fromNormalizedByteArray("http://a.b/a".getBytes(Charsets.US_ASCII)));
	}

	@Test
	public void testMalformed() {
		assertNull(BURL.parse(":a))/"));
	}

	@Test
	public void testResolution() {
		assertEquals("http://a/b", BURL.parse("http://a/").resolve(BURL.parse("/b")).toString());
		assertEquals("http://example.com/b", BURL.parse("http://example.com/a/").resolve(BURL.parse("../b")).toString());
		assertEquals("http://b/c", BURL.parse("http://a/").resolve(BURL.parse("http://b/c")).toString());
		assertEquals("http://foo.com/", BURL.parse("http://example.com/").resolve(BURL.parse("//foo.com")).toString());
	}


	@Test
	public void testTrailingDot() {
		assertEquals("a", BURL.parse("http://a./b").getHost());
	}

	@Test
	public void testFragmentCancellation() {
		assertEquals("http://a/b", BURL.parse("http://a/b#c").toString());
		assertEquals("a/b", BURL.parse("a/b#c").toString());
	}

	@Test
	public void testOpaque() {
		assertNull(BURL.parse("mailto:me"));
	}

	@Test
	public void testNullAuthority() {
		assertNull(BURL.parse("file:///test.html"));
	}

	@Test
	public void testHostFromSchemeAndAuthority() {
		assertEquals("example.com", BURL.hostFromSchemeAndAuthority("http://example.com".getBytes(Charsets.ISO_8859_1)));
		assertEquals("example.com", BURL.hostFromSchemeAndAuthority("http://username:password@example.com:42".getBytes(Charsets.ISO_8859_1)));
		assertEquals("example.com", BURL.hostFromSchemeAndAuthority("http://username:password@example.com".getBytes(Charsets.ISO_8859_1)));
		assertEquals("example.com", BURL.hostFromSchemeAndAuthority("http://username@example.com".getBytes(Charsets.ISO_8859_1)));
		assertEquals("example.com", BURL.hostFromSchemeAndAuthority("http://example.com:42".getBytes(Charsets.ISO_8859_1)));
	}

	public static String hostWithStartEnd(byte[] url) {
		final int startOfHost = BURL.startOfHost(url);
		final int lengthOfHost = BURL.lengthOfHost(url, startOfHost);
		return new String(url, startOfHost, lengthOfHost, Charsets.ISO_8859_1);
	}

	@Test
	public void testHostStartEnd() {
		assertEquals("example.com", hostWithStartEnd("http://example.com/".getBytes(Charsets.ISO_8859_1)));
		assertEquals("example.com", hostWithStartEnd("http://username:password@example.com:42/".getBytes(Charsets.ISO_8859_1)));
		assertEquals("example.com", hostWithStartEnd("http://username:password@example.com/".getBytes(Charsets.ISO_8859_1)));
		assertEquals("example.com", hostWithStartEnd("http://username@example.com/".getBytes(Charsets.ISO_8859_1)));
		assertEquals("example.com", hostWithStartEnd("http://example.com:42/".getBytes(Charsets.ISO_8859_1)));
		assertEquals("example.com", hostWithStartEnd("http://example.com/:".getBytes(Charsets.ISO_8859_1)));
	}

	@Test
	public void testToString() {
		assertEquals("http://example.com/", Util.toString(BURL.toByteArray(URI.create("http://example.com/"))));
	}

	@Test
	public void testPathAndQuery() {
		assertEquals("/a?b", BURL.pathAndQuery(BURL.parse("http://example.com/a?b")));
		assertEquals("/a", BURL.pathAndQuery(BURL.parse("http://example.com/a")));
	}

	@Test
	public void testReplacements() {
		assertEquals("http://a/b", BURL.parse("http:\\\\a\\b").toString());
		assertEquals("http://a/%25", BURL.parse("http://a/%").toString());
		assertEquals("http://a/%254", BURL.parse("http://a/%4").toString());
		assertEquals("http://a/%3F", BURL.parse("http://a/%3F").toString());
		assertEquals("http://a/%5E", BURL.parse("http://a/^").toString());
	}

	@Test
	public void testPercentNormalization() {
		assertEquals("http://a/%25", BURL.parse("http://a/%").toString());
		assertEquals("http://a/%25e", BURL.parse("http://a/%e").toString());
		assertEquals("http://a/%3F", BURL.parse("http://a/%3f").toString());
		assertEquals("http://a/%3F", BURL.parse("http://a/%3F").toString());
		assertEquals("http://a/%5E", BURL.parse("http://a/^").toString());
	}

	@Test
	public void testNonEscaping() {
		assertEquals("http://example.com/s/http%3A%2F%2Fbad.com", BURL.parse("http://example.com/s/http%3A%2F%2Fbad.com").toString());
	}

	@Test
	public void testSchemeAndAuthorityAsByteArrayFromByteArray() {
		assertArrayEquals("http://example.com".getBytes(Charsets.ISO_8859_1), BURL.schemeAndAuthorityAsByteArray(BURL.toByteArray(BURL.parse("http://example.com/a/"))));
		assertArrayEquals("http://user@example.com".getBytes(Charsets.ISO_8859_1), BURL.schemeAndAuthorityAsByteArray(BURL.toByteArray(BURL.parse("http://user@example.com/a/"))));
		assertArrayEquals("http://example.com:42".getBytes(Charsets.ISO_8859_1), BURL.schemeAndAuthorityAsByteArray(BURL.toByteArray(BURL.parse("http://example.com:42/a/"))));
		assertArrayEquals("http://user@example.com:42".getBytes(Charsets.ISO_8859_1), BURL.schemeAndAuthorityAsByteArray(BURL.toByteArray(BURL.parse("http://user@example.com:42/a/"))));
		assertArrayEquals("https://example.com".getBytes(Charsets.ISO_8859_1), BURL.schemeAndAuthorityAsByteArray(BURL.toByteArray(BURL.parse("https://example.com/a/:@"))));
	}

	@Test
	public void testDecomposition() {
		for(String url: new String[] { "http://example.com/", "http://example.com", "http://example.com/a/", "http://example.com/a/b#c", "http://example.com/a/b?q", "http://example.com/a/b?q#c" }) {
			assertEquals(BURL.parse(url),
					BURL.fromNormalizedSchemeAuthorityAndPathQuery(BURL.schemeAndAuthority(BURL.toByteArray(BURL.parse(url))),
							BURL.pathAndQueryAsByteArray(BURL.parse(url))));
			assertEquals(BURL.parse(url),
					BURL.fromNormalizedSchemeAuthorityAndPathQuery(BURL.schemeAndAuthority(BURL.toByteArray(BURL.parse(url))),
							BURL.pathAndQueryAsByteArray(BURL.toByteArray(BURL.parse(url)))));
			assertEquals(BURL.parse(url),
					BURL.fromNormalizedSchemeAuthorityAndPathQuery(BURL.schemeAndAuthority(BURL.parse(url)),
							BURL.pathAndQueryAsByteArray(BURL.parse(url))));
			assertEquals(BURL.parse(url),
					BURL.fromNormalizedSchemeAuthorityAndPathQuery(BURL.schemeAndAuthority(BURL.parse(url)),
							BURL.pathAndQueryAsByteArray(BURL.toByteArray(BURL.parse(url)))));
		}
	}

	@Test
	public void testCoppie() {
		assertNull(BURL.parse("http://coppie-.htmx.it"));
	}

	@Test
	public void testNo80() {
		assertEquals(BURL.parse("http://example.com/"), BURL.parse("http://example.com:80/"));
		assertEquals(BURL.parse("http://example.com/foo/bar.php?go=3&x=5"), BURL.parse("http://example.com:80/foo/bar.php?go=3&x=5"));
		assertNotEquals(BURL.parse("http://example.com/"), BURL.parse("http://example.com:85/"));
		assertNotEquals(BURL.parse("http://example.com/foo/bar.php?go=3&x=5"), BURL.parse("http://example.com:85/foo/bar.php?go=3&x=5"));
		assertNotEquals(BURL.parse("http://example.com:80/"), BURL.parse("http://example.com:85/"));
		assertNotEquals(BURL.parse("http://example.com/"), BURL.parse("http://example.com:85/"));
		assertEquals(BURL.parse("https://example.com/"), BURL.parse("https://example.com:443/"));
		assertEquals(BURL.parse("https://example.com/foo/bar.php?go=3&x=5"), BURL.parse("https://example.com:443/foo/bar.php?go=3&x=5"));
		assertNotEquals(BURL.parse("https://example.com:80/foo/bar.php?go=3&x=5"), BURL.parse("https://example.com:443/foo/bar.php?go=3&x=5"));
	}
}
