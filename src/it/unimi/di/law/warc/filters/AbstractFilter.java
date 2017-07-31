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

// RELEASE-STATUS: DIST

import org.apache.commons.lang.StringUtils;

/** An abstract implementation of a {@link Filter} providing a {@link #toString(Object...) method}
 * that helps in implementing properly {@link #toString()} for atomic (i.e., class-based) filters. */

public abstract class AbstractFilter<T> implements Filter<T> {

	/** A helper method that generates a string version of this filter (mainly
	 * useful for atomic, i.e., class-based, filters).
	 *
	 * <p>The output format is
	 *
	 *  <code>&lt;classname&gt;(&lt;arg&gt;, &lt;arg&gt;, ...)</code>
	 *
	 *  when &lt;classname&gt;
	 *  is the simple filter class name, if the filter class belongs
	 *  to the {@link #FILTER_PACKAGE_NAME} package, or the fully
	 *  qualified filter class name otherwise, and the arguments
	 *  are the string representations of the arguments of this method.
	 *
	 * @param arg arguments for the string representation above.
	 * @return the string representation specified above.
	 */
	protected String toString(final Object... arg) {
		// TODO: handle commas inside arguments
		if (this.getClass().getPackage().getName().equals(AbstractFilter.FILTER_PACKAGE_NAME))
			return this.getClass().getSimpleName() + "(" + StringUtils.join(arg, ',') + ")";
		else
			return this.getClass().getName() + "(" + StringUtils.join(arg, ',') + ")";
	}
}
