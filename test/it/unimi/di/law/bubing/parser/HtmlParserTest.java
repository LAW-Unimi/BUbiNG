package it.unimi.di.law.bubing.parser;

import static org.junit.Assert.assertArrayEquals;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.junit.Test;

import com.google.common.base.Charsets;

import it.unimi.di.law.bubing.parser.Parser.LinkReceiver;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.warc.util.StringHttpMessages;

//RELEASE-STATUS: DIST

public class HtmlParserTest {

	public final static String document1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document2Like1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/kxxx.php\";\n" + // Change, not relevant
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<tiTLE id=\"mummu\" special-type=\"liturchi\">Sebastiano Vigna</title>\n" + // Change, not relevant
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring xxxxediqne\"> and not this one\n" + // Change, not relevant
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document3Unlike1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye THIS IS A DIFFERENCE IN THE TEXT bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document4Unlike1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\na" + //A SMALL DIFFERENCE: just a single non-whitespace character
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document4bisLike1 =
			"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
			"\n" +
			"<html>\n" +
			"<head>\n" +
			"<style type=\"text/css\">\n" +
			"@import \"/css/content.php\";\n" +
			"@import \"/css/layout.php\";\n" +
			"</style>" +
			"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
			"</HEAD>\n" +
			"<boDY>\n" +
			"<div id=header>:::Sebastiano Vigna</div>" +
			"<div id=left>\n" +
			"<ul id=\"left-nav\">" +
			"<br>Bye bye baby\n " + //A SMALL IRRELEVANT DIFFERENCE: whitespace is coalesced
			"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
			"\n\n even whitespace counts \n\n" +
			"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
			"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
			"</body>\n" +
			"</html>";

	public final static String document5Unlike1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"a/aFrameSource\">The frame source counts</frame>\n" + // A difference in the source should count!
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document6Like5 = // Should be the same as document5Unlike1, if URL of the latter is xxx/a and of this is xxx
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"aFrameSource\">The frame source counts</frame>\n" + // A difference in the source should count!
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document7prefix =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n";

	public final static String document7suffix =
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document =
			"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
			"\n" +
			"<html>\n" +
			"<head>\n" +
			"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
			"</HEAD>\n" +
			"<boDY>\n" +
			"<div id=header>:::Sebastiano Vigna</div>" +
			"<div id=left>\n" +
			"<a href=\"http://nofollow.com/\" rel=nofollow hre>\n" +
			"<a href=\"http://nothing.com/\">\n" +
			"<a href=\"http://follow.com/\" rel=follow hre>\n" +
			"<ul id=\"left-nav\">" +
			"</body>\n" +
			"</html>";


	private static String[] allDocs = { document1, document2Like1, document3Unlike1, document4Unlike1, document5Unlike1, document6Like5 };
	private static String[] allURLs = { "http://vigna.dsi.unimi.it/xxx/yyy/a.html", "http://vigna.dsi.unimi.it/", "http://vigna.dsi.unimi.it/bbb", "http://vigna.dsi.unimi.it/bbb.php", "http://vigna.dsi.unimi.it/a", "http://vigna.dsi.unimi.it/" };

	@Test
	public void testDocument1() throws NoSuchAlgorithmException, IOException {
		final HTMLParser<Void> parser = new HTMLParser<>("MD5");

		final byte[][] allDigests = new byte[allDocs.length][];

		for (int i = 0; i < allDocs.length; i++) {
			final URI uri = BURL.parse(allURLs[i]);
			allDigests[i] = parser.parse(uri, new StringHttpMessages.HttpResponse(allDocs[i]), Parser.NULL_LINK_RECEIVER);
		}

		assertArrayEquals(allDigests[0], allDigests[1]);
		assertFalse(Arrays.equals(allDigests[0], allDigests[2]));
		assertFalse(Arrays.equals(allDigests[0], allDigests[3]));
		assertFalse(Arrays.equals(allDigests[0], allDigests[4]));
		/* FIXME currently the next test fails because the derelativization feature of the SRC by Digester is not implemented; please
		 * uncomment the following line as soon as it is re-implemented. */
		//assertTrue(Arrays.equals(allDigests[4], allDigests[5]));
	}

	@Test
	public void test3xx() throws NoSuchAlgorithmException, IOException {
		final HTMLParser<Void> parser = new HTMLParser<>("MD5");

		final StringHttpMessages.HttpResponse httpResponse = new StringHttpMessages.HttpResponse(301, "Moved", "Foo".getBytes(Charsets.ISO_8859_1), ContentType.TEXT_HTML);
		httpResponse.addHeader(new BasicHeader("Location", "http://example.com/0"));
		final byte[] digest0 = parser.parse(BURL.parse("http://example.com/"), httpResponse, Parser.NULL_LINK_RECEIVER);
		httpResponse.removeHeaders("Location");
		httpResponse.addHeader(new BasicHeader("Location", "http://example.com/1"));
		final byte[] digest1 = parser.parse(BURL.parse("http://example.com/"), httpResponse, Parser.NULL_LINK_RECEIVER);
		assertFalse(Arrays.equals(digest0, digest1));
	}



	public void assertSameDigest(final String a, final String b) throws NoSuchAlgorithmException, IOException {
		assertDigest(BURL.parse("http://a"), a, BURL.parse("http://a"), b, true);
	}

	public void assertDifferentDigest(final String a, final String b) throws NoSuchAlgorithmException, IOException {
		assertDigest(BURL.parse("http://a"), a, BURL.parse("http://a"), b, false);
	}

	public void assertDigest(final URI prefixa, final String a, final URI prefixb, final String b, final boolean equal) throws NoSuchAlgorithmException, IOException {
		final HTMLParser<Void> parser = new HTMLParser<>("MD5");
		final byte[] hashCode0 = parser.parse(prefixa, new StringHttpMessages.HttpResponse(a), Parser.NULL_LINK_RECEIVER);
		final byte[] hashCode1 = parser.parse(prefixb, new StringHttpMessages.HttpResponse(b), Parser.NULL_LINK_RECEIVER);
		assertEquals(Boolean.valueOf(Arrays.equals(hashCode0, hashCode1)), Boolean.valueOf(equal));
	}

	@Test
	public void testDifferent() throws NoSuchAlgorithmException, IOException {
		assertDifferentDigest("a", "b");
		assertDifferentDigest("<a>", "<i>");
		assertDifferentDigest("<foo>", "</foo>");
		assertDifferentDigest("<frame src=a>", "<frame src=b>");
		assertDifferentDigest("<iframe src=a>", "<iframe src=b>");
		assertDigest(BURL.parse("http://a"), "x", BURL.parse("http://b"), "x", false);
	}

	@Test
	public void testSame() throws NoSuchAlgorithmException, IOException {
		assertSameDigest("<a b>", "<a c>");
		assertSameDigest("<foo>", "<bar>");
		assertSameDigest("<foo >", "<foo  >");
		assertSameDigest("<img src=a>", "<img src=b>");
		assertSameDigest("<i>ciao mamma</i>", "<I>ciao mamma</I>");
		assertSameDigest(document1, document4bisLike1);
		assertDigest(BURL.parse("http://a"), "x", BURL.parse("http://a"), "x", true);
	}

	@Test
	public void testWhitespaceAndDigits() throws NoSuchAlgorithmException, IOException {
		assertDifferentDigest("dog cat", "dogcat");
		assertSameDigest("dog cat", "dog      cat"); // Quantity of whitespace is irrelevant
		assertDifferentDigest("dog cat", " dog cat"); // Existence of whitespace is not irrelevant
		assertSameDigest("dog cat", "dog434123cat"); // Digit sequences are considered like a single whitespace
		assertSameDigest("dog cat", "dog434123 314324cat"); // Digit sequences are considered like a single whitespace
		assertDifferentDigest("dog cat", "dog4341d23cat");
		assertDifferentDigest("3dog cat", "dog cat"); // Digits are like whitespace
	}


	@Test
	public void testLongDocument() throws NoSuchAlgorithmException, IOException  {
		final Random r = new Random(0);
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < HTMLParser.CHAR_BUFFER_SIZE * (2 + r.nextInt(3)); i++) sb.append((char)(64 + r.nextInt(61)));
		final String document7 = document7prefix + sb.toString() + document7suffix;
		assertSameDigest(document7, document7);
		sb.setCharAt(sb.length() / 2, (char)(sb.charAt(sb.length() / 2) + 1));
		assertDifferentDigest(document7, sb.toString());
	}

	@Test
	public void testOutOfScript() throws NoSuchAlgorithmException, IOException  {
		assertSameDigest("<script>ma</script> jong", "<script>quit</script> jong");
		assertDifferentDigest("<script>ma</script></script> jang", "<script>quit</script></script> jong");
	}

	@Test
	public void testEmptyScript() throws NoSuchAlgorithmException, IOException  {
		assertDifferentDigest("<script src=fadfadsfas/>go", "<script src=fadfadsfas/>ga");
	}

	@Test
	public void testNoFollow() throws NoSuchAlgorithmException, IOException {
		HTMLParser<Void> parser = new HTMLParser<>(BinaryParser.forName("MurmurHash3"));
		LinkReceiver linkReceiver = new HTMLParser.SetLinkReceiver();
		parser.parse(URI.create("http://example.com/"), new StringHttpMessages.HttpResponse(document),linkReceiver);
		assertEquals(2, linkReceiver.size());
		parser = new HTMLParser<>(BinaryParser.forName("MurmurHash3"), null, false, true);
		linkReceiver = new HTMLParser.SetLinkReceiver();
		parser.parse(URI.create("http://example.com/"), new StringHttpMessages.HttpResponse(document),linkReceiver);
		assertEquals(3, linkReceiver.size());
	}


}
