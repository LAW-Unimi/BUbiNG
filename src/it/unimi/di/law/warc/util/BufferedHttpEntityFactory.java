package it.unimi.di.law.warc.util;

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

// RELEASE-STATUS: DIST

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.entity.BufferedHttpEntity;

/** An implementation of a {@link HttpEntityFactory} that returns a {@link BufferedHttpEntity}. */
public class BufferedHttpEntityFactory implements HttpEntityFactory {

	public static final BufferedHttpEntityFactory INSTANCE = new BufferedHttpEntityFactory();

	private BufferedHttpEntityFactory() {}

	@Override
	public HttpEntity newEntity(final HttpEntity from) throws IOException {
		return new BufferedHttpEntity(from);
	}

}
