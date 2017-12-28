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

import org.apache.http.HttpResponse;

// RELEASE-STATUS: DIST

/** A filter accepting only http responses whose content stream appears to be binary. */
public class IsProbablyBinary extends AbstractFilter<HttpResponse> {

	public static final IsProbablyBinary INSTANCE = new IsProbablyBinary();
	public static final int BINARY_CHECK_SCAN_LENGTH = 1000;
	/** The number of zeroes that must appear to cause the page to be considered probably
	 * binary. Some misconfigured servers emit one or two ASCII NULs at the start of their
	 * pages, so we use a relatively safe value. */
	public static final int THRESHOLD = 3;

	private IsProbablyBinary() {}

	/** This method implements a simple heuristic for guessing whether a page is binary.
	 *
	 * <P>The first {@link #BINARY_CHECK_SCAN_LENGTH} bytes are scanned: if we find more than
	 * {@link #THRESHOLD} zeroes, we deduce that this page is binary. Note that this works
	 * also with UTF-8, as no UTF-8 legal character encoding contains these characters (unless
	 * you're encoding 0, but this is not our case).
	 *
	 * @return <code>true</code> if this page has most probably a binary content.
	 * @throws NullPointerException if the page has no byte content.
	 */
	@Override
	public boolean apply(final HttpResponse httpResponse) {
		try {
			final InputStream content = httpResponse.getEntity().getContent();
			int count = 0;
			for(int i = BINARY_CHECK_SCAN_LENGTH; i-- != 0;) {
				final int b = content.read();
				if (b == -1) return false;
				if (b == 0 && ++count == THRESHOLD) return true;
			}
		}
		catch(final IOException shouldntReallyHappen) {
			throw new RuntimeException(shouldntReallyHappen);
		}
		return false;
	}

	/**
	 * Get a new <code>IsProbablyBinary</code> that will accept only http responses whose content stream appears to be binary.
	 *
	 * @param emptySpec an empty string.
	 * @return a new <code>IsProbablyBinary</code> that will accept only http responses whose content stream appears to be binary.
	 * @deprecated Please use {@link #valueOf()} instead.
	 */
	@Deprecated
	public static IsProbablyBinary valueOf(final String emptySpec) {
		if (emptySpec.length() > 0) throw new IllegalArgumentException();
		return INSTANCE;
	}

	/**
	 * Get a new <code>IsProbablyBinary</code> that will accept only http responses whose content stream appears to be binary.
	 *
	 * @return a new <code>IsProbablyBinary</code> that will accept only http responses whose content stream appears to be binary.
	 */
	public static IsProbablyBinary valueOf() {
		return INSTANCE;
	}

	/**
	 * A string representation of the state of this filter.
	 *
	 * @return the class name of this + "()"
	 */
	@Override
	public String toString() {
		return getClass().getSimpleName() + "()";
	}

	@Override
	public Filter<HttpResponse> copy() {
		return this;
	}
}
