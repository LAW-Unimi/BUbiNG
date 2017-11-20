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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;

// RELEASE-STATUS: DIST

/** A filter accepting only http responses whose content stream (in ISO-8859-1 encoding) matches a regular expression. */
public class ResponseMatches extends AbstractFilter<HttpResponse> {

	private final Pattern pattern;

	public ResponseMatches(final Pattern pattern) {
		this.pattern = pattern;
	}

	/** Checks whether the response associated with this page matches (in ISO-8859-1 encoding)
	 * the regular expression provided at construction time.
	 *
	 * @return <code>true</code> if the response associated with this page matches (in ISO-8859-1 encoding)
	 * the regular expression provided at construction time.
	 * @throws NullPointerException if the page has no byte content.
	 */
	@Override
	public boolean apply(final HttpResponse httpResponse) {
		try {
			final InputStream content = httpResponse.getEntity().getContent();
			return pattern.matcher(IOUtils.toString(content, StandardCharsets.ISO_8859_1)).matches();
		}
		catch(IOException shouldntReallyHappen) {
			throw new RuntimeException(shouldntReallyHappen);
		}
	}

	/**
	 * Get a new content matcher that will accept only responses whose content stream matches the regular expression.
	 *
	 * @param spec a {@link java.util.regex} regular expression.
	 * @return a new content matcher that will accept only responses whose content stream matches the regular expression.
	 */
	public static ResponseMatches valueOf(final String spec) {
		return new ResponseMatches(Pattern.compile(spec));
	}

	/**
	 * A string representation of the state of this filter.
	 *
	 * @return the class name of this + "()"
	 */
	@Override
	public String toString() {
		return getClass().getSimpleName() + "(" + pattern.toString() + ")";
	}

	@Override
	public Filter<HttpResponse> copy() {
		return new ResponseMatches(pattern);
	}
}
