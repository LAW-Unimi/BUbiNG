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
