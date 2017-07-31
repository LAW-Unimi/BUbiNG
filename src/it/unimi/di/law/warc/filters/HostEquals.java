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

// RELEASE-STATUS: DIST

/** A filter accepting only URIs whose host equals (case-insensitively) a certain string.
 *
 *  <p>Note that {@link #apply(URI)} will throw an {@link IllegalArgumentException}
 *  if the argument has a {@code null} {@linkplain URI#getHost() host}.
 */
public class HostEquals extends AbstractFilter<URI> {

	/** The accepted host. */
	private final String host;

	/** Creates a filter that only accepts URLs with a given host.
	 *
	 * @param host the accepted host.
	 */
	public HostEquals(final String host) {
		this.host = host;
	}

	/**
	 * Apply the filter to a given URI
	 *
	 * @param uri the URI to be filtered
	 * @return true if the host part of <code>uri</code> is equal to the inner host
	 */
	@Override
	public boolean apply(final URI uri) {
		if (uri.getHost() == null) throw new IllegalArgumentException("URI \"" + uri + "\" has no host");
		// BURL hosts are always lower cased
		return uri.getHost().equals(host);
	}

	/**
	 * Get a new <code>HostEquals</code> that will accept only URIs whose host part is equal to spec
	 *
	 * @param spec a string that will be used to compare host parts of the URI
	 * @return a new <code>HostEquals</code> that will accept only URIs whose host is spec
	 */
	public static HostEquals valueOf(String spec) {
		return new HostEquals(spec);
	}

	/**
	 * A string representation of the state of this object, that is just the host allowed.
	 *
	 * @return the string that is the host of the URIs allowed by this filter
	 */
	@Override
	public String toString() {
		return toString(host);
	}

	/**
	 * Compare this object with a given generic one
	 *
	 * @return <code>true</code> if <code>x</code> is an instance of <code>HostEquals</code> and the URIs allowed by <code>x</code> are allowed by this and vice versa
	 */
	@Override
	public boolean equals(Object x) {
		if (x instanceof HostEquals) return ((HostEquals)x).host.equals(host);
		else return false;
	}

	@Override
	public int hashCode() {
		return host.hashCode() ^ HostEquals.class.hashCode();
	}

	@Override
	public Filter<URI> copy() {
		return this;
	}
}
