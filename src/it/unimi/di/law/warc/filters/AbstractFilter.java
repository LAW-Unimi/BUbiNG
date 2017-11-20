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
