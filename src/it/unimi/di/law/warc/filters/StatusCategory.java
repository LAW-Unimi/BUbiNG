package it.unimi.di.law.warc.filters;

import org.apache.http.HttpResponse;

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

// RELEASE-STATUS: DIST

/** A filter accepting only fetched response whose status category (status/100) has a certain value.
 */
public class StatusCategory extends AbstractFilter<HttpResponse> {

	/** The accepted category (e.g., 2 for 2xx). */
	private final int category;

	/** Creates a filter that only accepts responses of the given category.
	 *
	 * @param category the accepted category.
	 */
	public StatusCategory(final int category) {
		this.category = category;
	}

	/**
	 * Apply the filter to a given <code>HttpResponse</code>
	 *
	 * @param x the <code>HttpResponse</code> to be filtered
	 * @return <code>true</code> if the status code of <code>x</code> is equal to the category class allowed
	 */
	@Override
	public boolean apply(HttpResponse x) {
		return x.getStatusLine().getStatusCode() / 100 == category;
	}

	/**
	 * Get a new <code>StatusCategory</code> accepting only fetched response whose status category (status/100) has a given value
	 *
	 * @param spec a string
	 * @return a new <code>StatusCategory</code> accepting only fetched response whose status category (status/100) has value <code>spec</code>
	 */
	public static StatusCategory valueOf(String spec) {
		return new StatusCategory(Integer.parseInt(spec));
	}

	/**
	 * A string representation of this
	 *
	 * @return the categories allowed in string format
	 */
	@Override
	public String toString() {
		return toString(String.valueOf(category));
	}

	/**
	 * Compare this filter with a generic object
	 *
	 * @param x the object to be compared
	 * @return <code>true</code> if <code>x</code> is instance of <code>StatusCategory</code> and the category accepted by <code>x</code> is accepted by this and vice versa
	 */
	@Override
	public boolean equals(Object x) {
		if (x instanceof StatusCategory) return ((StatusCategory)x).category == category;
		else return false;
	}

	@Override
	public int hashCode() {
		return category ^ StatusCategory.class.hashCode();
	}

	@Override
	public Filter<HttpResponse> copy() {
		return this;
	}
}
