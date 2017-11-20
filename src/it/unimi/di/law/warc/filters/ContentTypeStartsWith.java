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
