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

/** A filter accepting only URIs whose overall length is below a given threshold. */
public class URLShorterThan extends AbstractFilter<URI> {

	/** URL longer than this threshold won't be accepted. */
	private final int threshold;

	/** Creates a filter that only accepts URLs shorter than the given threshold.
	 *
	 * @param threshold the acceptance threshold.
	 */
	public URLShorterThan(final int threshold) {
		this.threshold = threshold;
	}

	/**
	 * Apply the filter to the given URI
	 *
	 * @param uri the URL to be filtered
	 * @return true if <code>url</code> is shorter than the threshold
	 */
	@Override
	public boolean apply(final URI uri) {
		return uri.toString().length() < threshold;
	}

	/**
	 * Get a new <code>URLShorterThan</code>
	 *
	 * @param spec is the threshold that will be used by the filter
	 * @return a new <code>URLShorterThan</code> that will use <code>spec</code> as a threshold
	 */
	public static URLShorterThan valueOf(String spec) {
		return new URLShorterThan(Integer.parseInt(spec));
	}

	/**
	 * Get a string representation of this filter
	 *
	 * @return the threshold used by this filter in string format
	 */
	@Override
	public String toString() {
		return toString(Integer.toString(threshold));
	}

	/**
	 * Compare this with a given object
	 *
	 * @param x the object to be comared
	 * @return true if <code>x</code> is instance of <code>URLShorterThan</code> and <code>x</code> and this use the same threshold
	 */
	@Override
	public boolean equals(Object x) {
		return x instanceof URLShorterThan && ((URLShorterThan)x).threshold == threshold;
	}

	@Override
	public int hashCode() {
		return threshold ^ URLShorterThan.class.hashCode();
	}

	@Override
	public Filter<URI> copy() {
		return this;
	}
}
