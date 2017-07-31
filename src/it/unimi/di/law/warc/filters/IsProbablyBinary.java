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
		catch(IOException shouldntReallyHappen) {
			throw new RuntimeException(shouldntReallyHappen);
		}
		return false;
	}

	/**
	 * Get a new <code>IsProbablyBinary</code> that will accept only http responses whose content stream appears to be binary.
	 *
	 * @param emptySpec an empty string.
	 * @return a new <code>IsProbablyBinary</code> that will accept only http responses whose content stream appears to be binary.
	 */
	public static IsProbablyBinary valueOf(final String emptySpec) {
		if (emptySpec.length() > 0) throw new IllegalArgumentException();
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
