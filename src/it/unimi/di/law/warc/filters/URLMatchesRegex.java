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
