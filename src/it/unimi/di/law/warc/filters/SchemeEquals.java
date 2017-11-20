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

// RELEASE-STATUS: DIST

/** A filter accepting only URIs whose scheme equals a certain string (typically, <code>http</code>).
 *
 * <p>Note that {@link #apply(URI)} will throw an {@link IllegalArgumentException}
 * if the argument has a {@code null} {@linkplain URI#getScheme() scheme}.
 */
public class SchemeEquals extends AbstractFilter<URI> {

	/** The accepted scheme. */
	private final String scheme;

	/** Creates a filter that only accepts URIs with a given scheme.
	 *
	 * @param scheme the accepted scheme.
	 */
	public SchemeEquals(final String scheme) {
		this.scheme = scheme;
	}

	/**
	 * Apply the filter to a given URI
	 *
	 * @param uri the URI to be filtered
	 * @return <code>true</code> if uri has scheme equals to the inner string
	 */
	@Override
	public boolean apply(final URI uri) {
		if (uri.getScheme() == null) throw new IllegalArgumentException("URI \"" + uri + "\" has no scheme");
		return scheme.equals(uri.getScheme());
	}

	/**
	 * Get a new SchemeEquals accepting only URIs whose scheme equals the given string
	 *
	 * @param spec the scheme allowed
	 * @return A new SchemeEquals accepting only URIs whose scheme equals <code>spec</code>
	 */
	public static SchemeEquals valueOf(String spec) {
		return new SchemeEquals(spec);
	}

	/**
	 * A string representation of this
	 *
	 * @return the scheme allowed by this filter
	 */
	@Override
	public String toString() {
		return toString(scheme);
	}

	/**
	 * Compare a given object with this
	 *
	 * @param x an object to be compared
	 * @return <code>true</code> if <code>x</code> is an instance of <code>SchemeEquals</code> and the scheme allowed by this is the same of the one allowed by <code>x</code>
	 */
	@Override
	public boolean equals(Object x) {
		if (x instanceof SchemeEquals) return ((SchemeEquals)x).scheme.equals(scheme);
		else return false;
	}

	@Override
	public int hashCode() {
		return scheme.hashCode() ^ SchemeEquals.class.hashCode();
	}

	@Override
	public Filter<URI> copy() {
		return this;
	}
}
