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

import java.net.URI;
import java.util.regex.Pattern;

// RELEASE-STATUS: DIST

/** A filter accepting only URIs that match a certain regular expression. */
public class URLMatchesRegex extends AbstractFilter<URI> {

	/** The pattern containing the compiled regular expression. */
	private Pattern pattern;

	/** Creates a filter that only accepts URLs matching a given regular expression.
	 *
	 * @param expr the regular expression to be compiled
	 */
	public URLMatchesRegex(final String expr) {
		pattern = Pattern.compile(expr);
	}

	/**
	 * Creates a filter that only accepts URLs matching a given regular expression.
	 *
	 * @param pattern the compiled regular expression to be matched
	 */
	private URLMatchesRegex(final Pattern pattern) {
		this.pattern = pattern;
	}

	/**
	 * Apply the filter to a given URI
	 *
	 * @param uri the URI to be filtered
	 * @return <code>true</code> if <code>uri</code> matches the allowed pattern
	 */
	@Override
	public boolean apply(final URI uri) {
		return pattern.matcher(uri.toString()).matches();
	}

	/**
	 * Get a new <code>URLMatchesRegex</code> accepting only URIs that match a certain regular expression
	 *
	 * @param spec the expression in string format (to be compiled) accepted by the new <code>URLMatchesRegex</code>
	 * @return a new <code>URLMatchesRegex</code>
	 */
	public static URLMatchesRegex valueOf(String spec) {
		return new URLMatchesRegex(spec);
	}

	/**
	 * Get a string representation of this
	 *
	 * @return the allowed pattern in string format
	 */
	@Override
	public String toString() {
		return toString(pattern.pattern());
	}

	/**
	 * Compare this with a given object
	 *
	 * @param x the object to be compared
	 * @return <code>true</code> if <code>x</code> is instance of <code>URLMatchesRegex</code> and the pattern accepted by <code>x</code> is equal to the one accepted by this
	 */
	@Override
	public boolean equals(Object x) {
		if (x instanceof URLMatchesRegex) return ((URLMatchesRegex)x).pattern.equals(pattern);
		else return false;
	}

	@Override
	public int hashCode() {
		return pattern.hashCode() ^ URLMatchesRegex.class.hashCode();
	}

	@Override
	public Filter<URI> copy() {
		return new URLMatchesRegex(pattern); // TODO: check that we really need to make a copy.
	}
}
