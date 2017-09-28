package it.unimi.di.law.warc.filters;

/*
 * Copyright (C) 2013-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.dsi.lang.FlyweightPrototype;

import java.net.URI;

import org.apache.http.HttpResponse;

import com.google.common.base.Predicate;

//RELEASE-STATUS: DIST

/** A filter is a strategy to decide whether to accept a given
 *  object or not. Typically <code>T</code> will be either a {@link URI}, an
 *  {@link HttpResponse} or a {@link URIResponse}. Technically it is identical to the Google Guava
 *  {@link Predicate} interface, but there are some conventions listed
 *  below that apply only to filters.
 *
 *  <p>By contract, every filter that is an instance of a non-anonymous
 *  filter class is supposed to have a <strong>static</strong>
 *  method with the following signature
 *  <pre>public static Filter&lt;T&gt; valueOf(String x)</pre>
 *  that returns a filter (typically, a filter of its own kind) from
 *  a string. Moreover, it is required, for every filter class <code>F</code>
 *  and for every instance <code>f</code>, that <code>toString()</code> returns
 *  <pre><var>classname</var>(<var>spec</var>)</pre>
 *  where <pre>f.equals(F.valueOf(<var>spec</var>))</pre>
 *
 *  <p>Note that <code><var>classname</var></code> can omit the package name if
 *  it is {@link #FILTER_PACKAGE_NAME}.
 */

public interface Filter<T> extends Predicate<T>, FlyweightPrototype<Filter<T>> {

	/** The name of the package that contains this interface as well as
	 *  most filters.
	 */
	public final static String FILTER_PACKAGE_NAME = Filter.class.getPackage().getName();
}
