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

import org.apache.http.Header;
import org.apache.http.HttpResponse;

import com.google.common.net.HttpHeaders;

// RELEASE-STATUS: DIST

/** A filter accepting only fetched response whose content type starts with a given string.
 *
 *  <p>Typical usage: <code>ContentTypeStartsWith(text/)</code>,
 */
public class ContentTypeStartsWith extends AbstractFilter<HttpResponse> {

	/** The prefix of accepted content types. */
	private final String prefix;

	/**
	 * Creates a filter that only accepts URLs whose content type starts with a given prefix.
	 *
	 * @param prefix the prefix of the content type of the <code>HttpResponse</code> to be accepted
	 */
	public ContentTypeStartsWith(final String prefix) {
		this.prefix = prefix;
	}

	/**
	 * Apply the filter to a <code>HttpResponse</code>
	 *
	 * @param x the <code>HttpResponse</code> to be filtered
	 * @return <code>true</code> if the first header of the given <code>HttpResponse</code> is not null and starts with the prefix
	 */
	@Override
	public boolean apply(HttpResponse x) {
		final Header header = x.getFirstHeader(HttpHeaders.CONTENT_TYPE);
		return header != null && header.getValue().startsWith(prefix);
	}

	/**
	 * Get a new <code>ContentTypeStartsWith</code> that will accept only fetched responses whose content type starts with a given string
	 *
	 * @param spec a String, that will be used to compare prefixes.
	 * @return a new <code>ContentTypeStartsWith</code> that will accept only fetched response whose content type starts with <code>spec</code>
	 */
	public static ContentTypeStartsWith valueOf(String spec) {
		return new ContentTypeStartsWith(spec);
	}

	/**
	 * A string representation of the state of this object, that is just the prefix allowed.
	 *
	 * @return the string used by this object to compare prefixes
	 */
	@Override
	public String toString() {
		return toString(prefix);
	}

	/**
	 * Compare this object with a given generic one
	 *
	 * @return <code>true</code> if <code>x</code> is an instance of <code>ContentTypeStartsWith</code> and the prefix allowed by <code>x</code> is allowed by this and vice versa
	 */
	@Override
	public boolean equals(Object x) {
		return x instanceof ContentTypeStartsWith && ((ContentTypeStartsWith)x).prefix.equals(prefix);
	}

	@Override
	public Filter<HttpResponse> copy() {
		return this;
	}

	@Override
	public int hashCode() {
		return prefix.hashCode() ^ ContentTypeStartsWith.class.hashCode();
	}
}
