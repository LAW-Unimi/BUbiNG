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

import it.unimi.di.law.bubing.util.BURL;

import java.net.URI;

// RELEASE-STATUS: DIST

/** A filter accepting only a given URIs. */
public class URLEquals extends AbstractFilter<URI> {

	/** The URL to be matched. */
	private final URI uri;

	/** Creates a filter that only accepts URIs equal to a given URI.
	 *
	 * @param uri the URI that will be the only one accepted
	 */
	public URLEquals(final String uri) {
		this.uri = BURL.parse(uri);
		if (this.uri == null) throw new IllegalArgumentException("Unparsable URI " + uri);
	}

	/**
	 * Apply the filter to a given URI
	 *
	 * @param uri the URI to be filtered
	 * @return <code>true</code> if <code>uri</code> is equal to the URI to be matched
	 */
	@Override
	public boolean apply(final URI uri) {
		return uri.equals(uri);
	}

	/**
	 * Get a new <code>URLEquals</code> accepting only a given URI
	 *
	 * @param spec the URI to be matched
	 * @return a new instance of <code>URLEquals</code> that will accept only URIs equal to <code>spec</code>
	 */
	public static URLEquals valueOf(String spec) {
		return new URLEquals(spec);
	}

	/**
	 * Get a string representation of this filter
	 *
	 * @return a string containing the URI to be matched
	 */
	@Override
	public String toString() {
		return toString(uri);
	}

	/**
	 * Compare this filter with a given object
	 *
	 * @param x the object to be compared
	 * @return <code>true</code> if <code>x</code> is instance of <code>URLEquals</code> and the URIs accepted by <code>x</code> are equal to the URIs accepted by this and vice versa
	 */
	@Override
	public boolean equals(Object x) {
		if (x instanceof URLEquals) return ((URLEquals)x).uri.equals(uri);
		else return false;
	}

	@Override
	public int hashCode() {
		return uri.hashCode() ^ URLEquals.class.hashCode();
	}

	@Override
	public Filter<URI> copy() {
		return this;
	}
}
