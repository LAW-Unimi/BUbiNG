package it.unimi.di.law.bubing.parser;

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

import it.unimi.di.law.warc.filters.Filter;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.dsi.fastutil.objects.ObjectSets;
import it.unimi.dsi.lang.FlyweightPrototype;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.http.HttpResponse;

// RELEASE-STATUS: DIST

/** A generic parser for {@link HttpResponse responses}. Every parser provides the following functionalities:
 *  <ul>
 *  	<li>it acts as a {@link Filter} that is able to decide whether it can parse a certain {@link URIResponse} or not (e.g.,
 *  	based on the declared <code>content-type</code> header etc.);
 *  	<li>while {@linkplain #parse(URI, HttpResponse, LinkReceiver) parsing}, it will send the links found in the document to the
 *  	specified {@link LinkReceiver}, that will typically accumulate them or send them to the appropriate class for processing;
 *  	<li>the {@link #parse(URI, HttpResponse, LinkReceiver) parsing} method will return a digest computed on a
 *  	(possibly) suitably modified version of the document (the way in which the document it is actually modified and
 *		the way in which the hash is computed is implementation-dependent and should be commented by the implementing classes);
 *  	<li>after parsing, a {@linkplain #guessedCharset() guess of the charset used for the document} will be made available.
 *  </ul>
 */
public interface Parser<T> extends Filter<URIResponse> {
	/**
	 * A class that can receive URLs discovered during parsing. It may be used to iterate over the
	 * URLs found in the current page, but what will be actually returned by the iterator is
	 * implementation-dependent. It can be assumed that {@link #init(URI)} is called before every
	 * other method when parsing a page, exactly once per page.
	 */
	public static interface LinkReceiver extends Iterable<URI> {
		/**
		 * Handles the location defined by headers.
		 *
		 * @param location the location defined by headers.
		 */
		public void location(URI location);

		/**
		 * Handles the location defined by a <code>META</code> element.
		 *
		 * @param location the location defined by the <code>META</code> element.
		 */
		public void metaLocation(URI location);

		/**
		 * Handles the refresh defined by a <code>META</code> element.
		 *
		 * @param refresh the URL defined by the <code>META</code> element.
		 */
		public void metaRefresh(URI refresh);

		/**
		 * Handles a link.
		 *
		 * @param uri a link discovered during the parsing phase.
		 */
		public void link(URI uri);

		/**
		 * Initializes this receiver for a new page.
		 *
		 * @param responseUrl the URL of the page to be parsed.
		 */
		public void init(URI responseUrl);

		public int size();
	}

	/**
	 * A class that can receive piece of text discovered during parsing.
	 */
	public static interface TextProcessor<T> extends Appendable, FlyweightPrototype<TextProcessor<T>> {
		/**
		 * Initializes this processor for a new page.
		 *
		 * @param responseUrl the URL of the page to be parsed.
		 */
		public void init(URI responseUrl);

		/**
		 * Returns the result of the processing.
		 * @return the result of the processing.
		 */
		public T result();
	}


	/** A no-op implementation of {@link LinkReceiver}. */
	public final static LinkReceiver NULL_LINK_RECEIVER = new LinkReceiver() {
		@Override
		public void location(URI location) {}

		@Override
		public void metaLocation(URI location) {}

		@Override
		public void metaRefresh(URI refresh) {}

		@Override
		public void link(URI link) {}

		@Override
		public void init(URI responseUrl) {}

		@SuppressWarnings("unchecked")
		@Override
		public Iterator<URI> iterator() {
			return ObjectSets.EMPTY_SET.iterator();
		}

		@Override
		public int size() {
			return 0;
		}
	};

	/**
	 * Parses a response.
	 *
	 * @param response a response to parse.
	 * @param linkReceiver a link receiver.
	 * @return a digest of the page content, or {@code null} if no digest has been
	 * computed.
	 */
	public byte[] parse(final URI uri, final HttpResponse response, final LinkReceiver linkReceiver) throws IOException;

	/**
	 * Returns a guessed charset for the document, or {@code null} if the charset could not be
	 * guessed.
	 *
	 * @return a charset or {@code null}.
	 */
	public String guessedCharset();

	/**
	 * Returns the result of the processing.
	 *
	 * <p>Note that this method must be idempotent.
	 *
	 * @return the result of the processing.
	 */
	public T result();

	/** This method strengthens the return type of the method inherited from {@link Filter}.
	 *
	 * @return  a copy of this object, sharing state with this object as much as possible.
	 */
	@Override
	public Parser<T> copy();
}
