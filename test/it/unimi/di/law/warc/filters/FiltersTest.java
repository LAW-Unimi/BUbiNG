package it.unimi.di.law.warc.filters;

/*
 * Copyright (C) 2004-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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


/** A class to test {@link AbstractFilter}. */

public class FiltersTest {
	public static class StartsWithStringFilter extends AbstractFilter<String> {
		private String prefix;
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
		private String suffix;
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
		private int bound;
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
	public void testBooleanComposition() {
		AbstractFilter<String> iniziaConA = new StartsWithStringFilter("a");
		AbstractFilter<String> finisceConA = new EndsWithStringFilter("a");
		AbstractFilter<String> finisceConB = new EndsWithStringFilter("b");
		AbstractFilter<String> lungaPiuDi5 = new LongerThanStringFilter(5);

		Filter<String> composto = Filters.and(iniziaConA, Filters.or(finisceConA, finisceConB), Filters.not(lungaPiuDi5));

		assertTrue(composto.apply("ab"));
		assertTrue(composto.apply("addb"));
		assertTrue(composto.apply("adda"));
		assertFalse(composto.apply("dda"));
		assertFalse(composto.apply("adddddda"));
		assertFalse(composto.apply("ad"));
	}

	@Test
	public void testParsingTrue() throws ParseException {
		FilterParser<String> filterParser = new FilterParser<>(String.class);
		assertTrue(filterParser.parse("TRUE").apply(new String()));
	}

	@Test
	public void testParsing() throws ParseException {
		FilterParser<String> filterParser = new FilterParser<>(String.class);
		Filter<String> composto = filterParser.parse(
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
		FilterParser<URI> filterParser = new FilterParser<>(URI.class);

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
		FilterParser<URI> filterParser = new FilterParser<>(URI.class);
		Filter<URI> filter = filterParser.parse("DuplicateSegmentsLessThan(3)");
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

}
