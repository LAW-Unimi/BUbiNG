package it.unimi.di.law.warc.filters;

import java.lang.reflect.Method;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;

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

import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.bubing.util.Link;
import it.unimi.di.law.warc.filters.parser.ParseException;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.records.WarcRecord.Type;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.lang.FlyweightPrototype;

// RELEASE-STATUS: DIST

/** A collection of static methods to deal with {@link Filter filters}. */
public class Filters {

	public static final Filter<?>[] EMPTY_ARRAY = {};

	/** A set containing all filters in the bubing.filters package. This primitive technique
	 * is used to circumvent the impossibility of obtaining all classes in a package by reflection. */
	@SuppressWarnings("unchecked")
	private static final ObjectOpenHashSet<Class<? extends Filter<?>>> FILTERS = new ObjectOpenHashSet<>(
			// TODO: periodically check that this list is complete.
			new Class[] { ContentTypeStartsWith.class, DigestEquals.class, DuplicateSegmentsLessThan.class,
					HostEndsWith.class, HostEndsWithOneOf.class, HostEquals.class,
					IsHttpResponse.class, IsProbablyBinary.class, PathEndsWithOneOf.class,
					ResponseMatches.class, SchemeEquals.class, StatusCategory.class, URLEquals.class,
					URLMatchesRegex.class, URLShorterThan.class
					}
			);

	/** Produces the conjunction of the given filters.
	 *
	 * @param <T> the type of objects that the filters deal with.
	 * @param f the filters.
	 * @return the conjunction.
	 */
	@SafeVarargs
	public static<T> Filter<T> and(final Filter<T>... f) {
		return new Filter<T>() {
			@Override
			public boolean apply(final T x) {
				for (final Filter<T> filter: f) if (! filter.apply(x)) return false;
				return true;
			}

			@Override
			public String toString() {
				return "(" + StringUtils.join(f, " and ") + ")";
			}

			@Override
			public Filter<T> copy() {
				return Filters.and(Filters.copy(f));
			}
		};
	}

	/** Produces the disjunction of the given filters.
	 *
	 * @param <T> the type of objects that the filters deal with.
	 * @param f the filters.
	 * @return the disjunction.
	 */
	@SafeVarargs
	public static<T> Filter<T> or(final Filter<T>... f) {
		return new Filter<T>() {
			@Override
			public boolean apply(final T x) {
				for (final Filter<T> filter: f) if (filter.apply(x)) return true;
				return false;
			}

			@Override
			public String toString() {
				return "(" + StringUtils.join(f, " or ") + ")";
			}

			@Override
			public Filter<T> copy() {
				return Filters.or(Filters.copy(f));
			}

		};
	}

	/** Produces the negation of the given filter.
	 *
	 * @param <T> the type of objects that the filter deal with.
	 * @param filter the filter.
	 * @return the negation of the given filter.
	 */
	public static<T> Filter<T> not(final Filter<T> filter) {
		return new AbstractFilter<T>() {
			@Override
			public boolean apply(final T x) {
				return ! filter.apply(x);
			}

			@Override
			public String toString() {
				return "(not " + filter + ")";
			}

			@Override
			public Filter<T> copy() {
				return not(filter.copy());
			}
		};
	}

	// TODO: change this to a static, correctly typed method.
	/** The constantly true filter. */
	@SuppressWarnings("rawtypes")
	public static Filter TRUE = new Filter() {
		@Override
		public boolean apply(Object x) {
			return true;
		}

		@Override
		public String toString() {
			return "true";
		}

		@Override
		public FlyweightPrototype copy() {
			return this;
		}
	};

	@SuppressWarnings("rawtypes")
	/** The constantly false filter. */
	public static Filter FALSE = new Filter() {
		@Override
		public boolean apply(Object x) {
			return false;
		}

		@Override
		public String toString() {
			return "false";
		}

		@Override
		public FlyweightPrototype copy() {
			return this;
		}
	};


	/** Creates a filter from a filter class name and an external form.
	 *
	 * @param className the name of a filter class; it may either be a single class name (in which case it
	 * 	will be qualified with {@link Filter#FILTER_PACKAGE_NAME}) or a fully qualified classname.
	 * @param spec the specification from which the filter will be created, using the <tt>valueOf(String)</tt> method (see {@link Filter}).
	 * @param tClass the base class of the filter that is desired: it should coincide with <code>T</code>; if the base type <code>D</code> of
	 *  the filter is wrong, it will try to adapt it by using a static method in the Filters class whose signature is
	 *  <pre>public static Filter&lt;T&gt; adaptD2T(Filter&lt;D&gt;)</pre>.
	 * @return the filter.
	 */
	@SuppressWarnings("unchecked")
	public static<T> Filter<T> getFilterFromSpec(String className, String spec, Class<T> tClass) throws ParseException {
		String filterClassName;

		if (className.indexOf('.') >= 0) filterClassName = className;
		else filterClassName = Filter.FILTER_PACKAGE_NAME + "." + className;
		try {
			// Produce the filter
			final Class<?> c = Class.forName(filterClassName);
			if (! Filter.class.isAssignableFrom(c)) throw new ParseException(filterClassName + " is not a valid filter class");
			// Empty spec, empty valueOf()
			final Filter<T> filter = spec.length() != 0
					? (Filter<T>)c.getMethod("valueOf", String.class).invoke(null, spec)
					: (Filter<T>)c.getMethod("valueOf").invoke(null);

			// Extract its base type
			final Method method[] = filter.getClass().getMethods();
			int i;
			for (i = 0; i < method.length; i++) if (! method[i].isSynthetic() && method[i].getName().equals("apply")) break;
			if (i == method.length) throw new NoSuchMethodException("Could not find apply method in filter " + filter);
			final Class<?>[] parameterTypes = method[i].getParameterTypes();
			if (parameterTypes.length != 1) throw new NoSuchMethodException("Could not find one-argument apply method in filter " + filter);
			final Class<?> toClass = parameterTypes[0];

			// Possibly: adapt the filter
			if (toClass.equals(tClass)) return filter;
			else {
				Method adaptMethod;
				try {
					adaptMethod = Filters.class.getMethod("adaptFilter" + toClass.getSimpleName() + "2" + tClass.getSimpleName(), Filter.class);
				} catch (final NoSuchMethodException e) {
					throw new NoSuchMethodException("Cannot adapt a Filter<" + toClass.getSimpleName() + "> into Filter<" + tClass.getSimpleName() + ">");
				}
				return (Filter<T>)adaptMethod.invoke(null, filter);
			}
		}
		catch(final ParseException e) {
			throw e;
		}
		catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/** Adapts a filter with {@link String} base type to a filter with {@link URI} base type. For testing purposes only.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<URI> adaptFilterString2URI(final Filter<String> original) {
		return new AbstractFilter<URI>() {
			@Override
			public boolean apply(final URI uri) {
				return original.apply(uri.toString());
			}
			@Override
			public String toString() {
				return original.toString();
			}
			@Override
			public Filter<URI> copy() {
				return adaptFilterString2URI(original.copy());
			}
		};
	}

	/** Adapts a filter with {@link URI} base type to a filter with {@link HttpResponseWarcRecord} base type.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<HttpResponseWarcRecord> adaptFilterURI2HttpResponseWarcRecord(final Filter<URI> original) {
		return new AbstractFilter<HttpResponseWarcRecord>() {
			@Override
			public boolean apply(final HttpResponseWarcRecord response) {
				return original.apply(response.getWarcTargetURI());
			}
			@Override
			public String toString() {
				return original.toString();
			}
			@Override
			public Filter<HttpResponseWarcRecord> copy() {
				return adaptFilterURI2HttpResponseWarcRecord(original.copy());
			}
		};
	}

	/** Adapts a filter with {@link URI} base type to a filter with {@link Link} base type,
	 * applying the original filter to the target URI.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<Link> adaptFilterURI2Link(final Filter<URI> original) {
		return new AbstractFilter<Link>() {
			@Override
			public boolean apply(final Link link) {
				return original.apply(link.target);
			}
			@Override
			public String toString() {
				return original.toString();
			}
			@Override
			public Filter<Link> copy() {
				return adaptFilterURI2Link(original.copy());
			}
		};
	}

	/** Adapts a filter with {@link URI} base type to a filter with {@link WarcRecord} base type.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<WarcRecord> adaptFilterURI2WarcRecord(final Filter<URI> original) {
		return new AbstractFilter<WarcRecord>() {
			@Override
			public boolean apply(WarcRecord x) {
				return original.apply(x.getWarcTargetURI()); // TODO: PORTING: can be null, should we handle the case here?
			}
			@Override
			public String toString() {
				return original.toString();
			}
			@Override
			public Filter<WarcRecord> copy() {
				return adaptFilterURI2WarcRecord(original.copy());
			}
		};
	}

	/** Adapts a filter with {@link HttpResponse} base type to a filter with {@link WarcRecord} base type.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<WarcRecord> adaptFilterHttpResponse2WarcRecord(final Filter<HttpResponse> original) {
		return new AbstractFilter<WarcRecord>() {
			@Override
			public boolean apply(WarcRecord x) {
				if (x.getWarcType() == Type.RESPONSE) return original.apply((HttpResponseWarcRecord)x);
				else return false;
			}
			@Override
			public String toString() {
				return original.toString();
			}
			@Override
			public Filter<WarcRecord> copy() {
				return adaptFilterHttpResponse2WarcRecord(original.copy());
			}
		};
	}

	/** Adapts a filter with {@link HttpResponse} base type to a filter with {@link FetchData} base type.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<FetchData> adaptFilterHttpResponse2FetchData(final Filter<HttpResponse> original) {
		return new AbstractFilter<FetchData>() {
			@Override
			public boolean apply(FetchData x) {
				return original.apply(x.response());
			}
			@Override
			public String toString() {
				return original.toString();
			}
			@Override
			public Filter<FetchData> copy() {
				return adaptFilterHttpResponse2FetchData(original.copy());
			}
		};
	}

	/** Adapts a filter with {@link URI} base type to a filter with {@link FetchData} base type.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<FetchData> adaptFilterURI2FetchData(final Filter<URI> original) {
		return new AbstractFilter<FetchData>() {
			@Override
			public boolean apply(FetchData x) {
				return original.apply(x.uri());
			}
			@Override
			public String toString() {
				return original.toString();
			}
			@Override
			public Filter<FetchData> copy() {
				return adaptFilterURI2FetchData(original.copy());
			}
		};
	}

	/** Adapts a filter with {@link HttpResponse} base type to a filter with {@link URIResponse} base type.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<URIResponse> adaptFilterHttpResponse2URIResponse(final Filter<HttpResponse> original) {
		return new AbstractFilter<URIResponse>() {
			@Override
			public boolean apply(URIResponse x) {
				return original.apply(x.response());
			}
			@Override
			public String toString() {
				return original.toString();
			}
			@Override
			public Filter<URIResponse> copy() {
				return adaptFilterHttpResponse2URIResponse(original.copy());
			}
		};
	}

	/** Adapts a filter with {@link URI} base type to a filter with {@link URIResponse} base type.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static AbstractFilter<URIResponse> adaptFilterURI2URIResponse(final Filter<URI> original) {
		return new AbstractFilter<URIResponse>() {
			@Override
			public boolean apply(URIResponse x) {
				return original.apply(x.uri());
			}
			@Override
			public String toString() {
				return original.toString();
			}
			@Override
			public Filter<URIResponse> copy() {
				return adaptFilterURI2URIResponse(original.copy());
			}
		};
	}
	/** Returns a list of the standard filter classes.
	 *
	 * @return a list of standard filter classes.
	 */
	public static Class<? extends Filter<?>>[] standardFilters() {
		return FILTERS.toArray(new Class[FILTERS.size()]);
	}


	@SafeVarargs
	public static<T> Filter<T>[] copy(final Filter<T>... f) {
		final Filter<T>[] result = f.clone();
		for (int i = 0; i < f.length; i++) f[i] = f[i].copy();
		return result;
	}
}
