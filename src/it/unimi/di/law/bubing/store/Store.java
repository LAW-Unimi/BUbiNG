package it.unimi.di.law.bubing.store;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpResponse;

//RELEASE-STATUS: DIST

/** An interface for components that are able to store pages. */

public interface Store extends Closeable {

	void store(final URI uri, final HttpResponse response, boolean isDuplicate, final byte[] contentDigest, final String guessedCharset) throws IOException, InterruptedException;

	@Override
	void close() throws IOException;
}
