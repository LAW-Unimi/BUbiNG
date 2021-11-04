package it.unimi.di.law.warc.filters;

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.junit.Test;

import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.Link;
import it.unimi.di.law.warc.filters.parser.FilterParser;
import it.unimi.di.law.warc.filters.parser.ParseException;



//RELEASE-STATUS: DIST


/** A class to test {@link AbstractFilter}. */

public class FiltersTest {
	public static class StartsWithStringFilter extends AbstractFilter<String> {
		private final String prefix;
		public StartsWithStringFilter(String prefix) { this.prefix = prefix; }
		@Override
		public boolean apply(String x) { return x.startsWith(prefix); }
		public static StartsWithStringFilter valueOf(String args) {
			return new StartsWithStringFilter(args);
		}
		@Override
		public String toString() {
			return toString(prefix);
		}
		@Override
		public Filter<String> copy() {
			return this;
		}
	}

	public static class EndsWithStringFilter extends AbstractFilter<String> {
		private final String suffix;
		public EndsWithStringFilter(String suffix) { this.suffix = suffix; }
		@Override
		public boolean apply(String x) { return x.endsWith(suffix); }
		public static EndsWithStringFilter valueOf(String args) {
			return new EndsWithStringFilter(args);
		}
		@Override
		public String toString() {
			return toString(suffix);
		}
		@Override
		public Filter<String> copy() {
			return this;
		}
	}

	public static class LongerThanStringFilter extends AbstractFilter<String> {
		private final int bound;
		public LongerThanStringFilter(int bound) { this.bound = bound; }
		@Override
		public boolean apply(String x) { return x.length() > bound; }
		public static LongerThanStringFilter valueOf(String args) {
			return new LongerThanStringFilter(Integer.parseInt(args));
		}
		@Override
		public String toString() {
			return toString(String.valueOf(bound));
		}
		@Override
		public Filter<String> copy() {
			return this;
		}
	}

	@Test
	public void testSameHost() throws ParseException {
		final FilterParser<Link> filterParser = new FilterParser<>(Link.class);
		final Filter<Link> filter = filterParser.parse("SameHost()");
		assertTrue(filter.apply(new Link(BURL.parse("http://example.com/a"), BURL.parse("http://example.com/b"))));
		assertFalse(filter.apply(new Link(BURL.parse("http://foo.com/"), BURL.parse("http://bar.com/"))));
	}


	@Test
	public void testBooleanComposition() {
		final AbstractFilter<String> iniziaConA = new StartsWithStringFilter("a");
		final AbstractFilter<String> finisceConA = new EndsWithStringFilter("a");
		final AbstractFilter<String> finisceConB = new EndsWithStringFilter("b");
		final AbstractFilter<String> lungaPiuDi5 = new LongerThanStringFilter(5);

		final Filter<String> composto = Filters.and(iniziaConA, Filters.or(finisceConA, finisceConB), Filters.not(lungaPiuDi5));

		assertTrue(composto.apply("ab"));
		assertTrue(composto.apply("addb"));
		assertTrue(composto.apply("adda"));
		assertFalse(composto.apply("dda"));
		assertFalse(composto.apply("adddddda"));
		assertFalse(composto.apply("ad"));
	}

	@Test
	public void testParsingTrue() throws ParseException {
		final FilterParser<String> filterParser = new FilterParser<>(String.class);
		assertTrue(filterParser.parse("TRUE").apply(new String()));
	}

	@Test
	public void testParsing() throws ParseException {
		final FilterParser<String> filterParser = new FilterParser<>(String.class);
		final Filter<String> composto = filterParser.parse(
				"it.unimi.di.law.warc.filters.FiltersTest$StartsWithStringFilter(a)" +
				" and " +
				"it.unimi.di.law.warc.filters.FiltersTest$EndsWithStringFilter(a) " +
				" or " +
				"it.unimi.di.law.warc.filters.FiltersTest$EndsWithStringFilter(b)"
			);
		System.out.println("TESTING: " + composto);
		assertTrue(composto.apply("aa"));
		assertTrue(composto.apply("bb"));
		assertFalse(composto.apply("dda"));
		assertFalse(composto.apply("add"));
	}

	@Test
	public void testURLParsing() throws ParseException {
		final FilterParser<URI> filterParser = new FilterParser<>(URI.class);

		Filter<URI> filter = filterParser.parse("HostEquals(www.dsi.unimi.it) or (HostEndsWith(.it) and not URLMatchesRegex(.*vigna.*))");
		System.out.println("TESTING: " + filter);
		assertTrue(filter.apply(BURL.parse("http://www.dsi.unimi.it/index.php")));
		assertTrue(filter.apply(BURL.parse("http://www.foo.it/index.php")));
		assertFalse(filter.apply(BURL.parse("http://www.vigna.foo.it/index.php")));
		assertFalse(filter.apply(BURL.parse("http://www.foo.com/index.php")));

		filter = filterParser.parse("PathEndsWithOneOf(html,htm,php) and not PathEndsWithOneOf(mahtml)");
		System.out.println("TESTING: " + filter);
		assertTrue(filter.apply(BURL.parse("http://www.dsi.unimi.it/index.php")));
		assertTrue(filter.apply(BURL.parse("http://www.foo.it/index.html")));
		assertFalse(filter.apply(BURL.parse("http://www.foo.it/index.mahtml")));
		assertTrue(filter.apply(BURL.parse("http://www.vigna.foo.it/index.PHP?sadmdsak")));
		assertFalse(filter.apply(BURL.parse("http://www.foo.com/a/b/c/index.jpg")));
	}

	@Test
	public void testDuplicateSegments() throws ParseException {
		final FilterParser<URI> filterParser = new FilterParser<>(URI.class);
		final Filter<URI> filter = filterParser.parse("DuplicateSegmentsLessThan(3)");
		System.out.println("TESTING: " + filter);
		assertFalse(filter.apply(BURL.parse("http://example.com/a/a/a/a/a")));
		assertFalse(filter.apply(BURL.parse("http://example.com/b/a/b/a/b/a/-")));
		assertFalse(filter.apply(BURL.parse("http://example.com/a/b/a/a/a")));
		assertTrue(filter.apply(BURL.parse("http://example.com/bbb/bbba/f/e")));
		assertFalse(filter.apply(BURL.parse("http://example.com/l/lc/i/c/l/lc/p/i/c/l/lc/p/l/lc/i/c/l/lc/p/i/c/l/lc/p/i/c/l/lc/p/")));
		assertTrue(filter.apply(BURL.parse("http://example.com/b/d/b/c/b/e")));
		assertFalse(filter.apply(BURL.parse("http://example.com/b/b/b")));
		assertFalse(filter.apply(BURL.parse("http://example.com/b/a/b/a/b/a/")));
		assertFalse(filter.apply(BURL.parse("http://example.com/b/a/b/a/b/a/-")));
		assertFalse(filter.apply(BURL.parse("http://example.com/foo/bar/foo/bar/foo/bar")));
		assertTrue(filter.apply(BURL.parse("http://example.com/b/a/b/a/b/c/b/a/")));
		assertTrue(filter.apply(BURL.parse("http://example.com/b/a/b/a/b/b/a/")));
		assertTrue(filter.apply(BURL.parse("http://example.com/b/b")));
		assertTrue(filter.apply(BURL.parse("http://a")));
		assertTrue(filter.apply(BURL.parse("http://example.com/")));
		assertTrue(filter.apply(BURL.parse("http://example.com/b")));
		assertTrue(filter.apply(BURL.parse("http://example.com/b/")));
		assertTrue(filter.apply(BURL.parse("http://example.com/b/b")));
		assertTrue(filter.apply(BURL.parse("http://example.com/b/b/")));
		assertFalse(filter.apply(BURL.parse("http://example.com/a/b/b/b")));
		assertFalse(filter.apply(BURL.parse("http://example.com/a/b/a/c/a/c/a/c")));
		assertFalse(filter.apply(BURL.parse("http://example.com/b/b/b/a")));
		assertFalse(filter.apply(BURL.parse("http://example.com/b/a/d/b/a/d/b/a/d")));
		assertFalse(filter.apply(BURL.parse("http://example.com/b/a/d/b/a/d/b/a/d/z")));
		assertTrue(filter.apply(BURL.parse("http://example.com/b/b/a/b/b/a/b/a")));
		assertFalse(filter.apply(BURL.parse("http://example.com/a/b/b/b")));
		assertFalse(filter.apply(BURL.parse("http://example.com/c/b/b/b")));
	}

	@Test
	public void testLinkAdapter() throws ParseException {
		final FilterParser<Link> filterParser = new FilterParser<>(Link.class);
		final Filter<Link> filter = filterParser.parse("HostEquals(www.dsi.unimi.it)");
		assertTrue(filter.apply(new Link(null, BURL.parse("http://www.dsi.unimi.it/index.php"))));
		assertFalse(filter.apply(new Link(null, BURL.parse("http://www.di.unimi.it/index.php"))));
	}
	
	@Test
	public void testIssue28() {
		URLMatchesRegex filter = new URLMatchesRegex(".*//.*[0-9][0-9]?[0-9]?\\.[0-9][0-9]?[0-9]?\\.[0-9][0-9]?[0-9]?.*");
		assertTrue(filter.apply(BURL.parse("http://example.com/?ip=123.123.123.213")));
	}
}
