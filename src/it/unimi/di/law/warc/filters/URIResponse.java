package it.unimi.di.law.warc.filters;

/*
 * Copyright (C) 2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;

import java.net.URI;

import org.apache.http.HttpResponse;

//RELEASE-STATUS: DIST

/** An interface implemented by all classes able to expose a {@link HttpResponse} and {@link URI}, e.g. {@link HttpResponseWarcRecord} or {@link FetchData}. */

public interface URIResponse {

	/** Returns the URI part.
	 *
	 * @return the URI.
	 */
	public URI uri();

	/** Returns the response part.
	 *
	 * @return the response.
	 */
	public HttpResponse response();

}
