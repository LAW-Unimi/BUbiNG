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

/** A filter accepting only URIs whose host ends with (case-insensitively) a certain suffix.
 *
 *  <p>Note that {@link #apply(URI)} will throw an {@link IllegalArgumentException}
 *  if the argument has a null {@linkplain URI#getHost() host}.
 */
public class HostEndsWith extends AbstractFilter<URI> {

	/** The accepted host suffix (lowercased). */
	private final String suffix;

	/** Creates a filter that only accepts URLs whose host part has a given suffix.
	 *
	 * @param suffix the accepted suffix.
	 */
	public HostEndsWith(final String suffix) {
		this.suffix = suffix.toLowerCase();
	}

	/**
	 * Apply the filter to a given URI
	 *
	 * @param uri the URI to be filtered
	 * @return <code>true</code> if the host part of <code>uri</code> ends with the allowed suffix
	 */
	@Override
	public boolean apply(final URI uri) {
		if (uri.getHost() == null) throw new IllegalArgumentException("URI \"" + uri + "\" has no host");
		// BURL hosts are always lower cased
		return uri.getHost().endsWith(suffix);
	}

	/**
	 * Get a new <code>HostEndsWith</code> that will accept only URIs whose suffix is given in input
	 *
	 * @param spec a string that will be used to compare suffixes
	 * @return a new <code>HostEndsWith</code> that will accept only URIs whose suffix is <code>spec</code>
	 */
	public static HostEndsWith valueOf(String spec) {
		return new HostEndsWith(spec);
	}

	/**
	 * A string representation of the state of this object, that is just the suffix allowed.
	 *
	 * @return the string used by this object to compare suffixes
	 */
	@Override
	public String toString() {
		return toString(suffix);
	}

	/**
	 * Compare this object with a given generic one
	 *
	 * @return true if <code>x</code> is an instance of <code>HostEndsWith</code> and the suffix allowed by <code>x</code> is allowed by this and vice versa
	 */
	@Override
	public boolean equals(Object x) {
		if (x instanceof HostEndsWith) return ((HostEndsWith)x).suffix.equals(suffix);
		else return false;
	}

	@Override
	public int hashCode() {
		return suffix.hashCode() ^ HostEndsWith.class.hashCode();
	}

	@Override
	public Filter<URI> copy() {
		return this;
	}
}
