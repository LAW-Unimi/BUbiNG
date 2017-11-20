package it.unimi.di.law.warc.filters;

/*
 * Copyright (C) 2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.bubing.util.Link;

// RELEASE-STATUS: DIST

/** A filter accepting only inter-host links. */
public class SameHost extends AbstractFilter<Link> {
	private static final SameHost INSTANCE = new SameHost();

	/**
	 * Apply the filter to a given link, returning true if source and target
	 * have the same {@linkplain java.net.URI#getHost() host}.
	 *
	 * @param link the link to be filtered.
	 * @return true if the host of the source and target of {@code link} are the same.
	 */
	@Override
	public boolean apply(final Link link) {
		final String sourceHost = link.source.getHost(), targetHost = link.target.getHost();
		if (sourceHost == null) throw new IllegalArgumentException("URI \"" + link.source + "\" has no host");
		if (targetHost == null) throw new IllegalArgumentException("URI \"" + link.target + "\" has no host");
		// BURL hosts are always lower cased
		return sourceHost.equals(targetHost);
	}

	/**
	 * Get a <code>SameHost</code> filter.
	 *
	 * @return a <code>SameHost</code> filter.
	 */
	public static SameHost valueOf() {
		return INSTANCE;
	}

	/**
	 * Returns {@code SameHost()}.
	 *
	 * @return {@code SameHost()}.
	 */
	@Override
	public String toString() {
		return "SameHost()";
	}

	/**
	 * Compare this object with a given generic one.
	 *
	 * @return true if <code>x</code> is an instance of <code>SameHost</code>.
	 */
	@Override
	public boolean equals(Object x) {
		return x instanceof SameHost;
	}

	@Override
	public int hashCode() {
		return SameHost.class.hashCode();
	}

	@Override
	public Filter<Link> copy() {
		return this;
	}
}
