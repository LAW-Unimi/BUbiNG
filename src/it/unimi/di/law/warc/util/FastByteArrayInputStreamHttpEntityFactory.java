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

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpEntity;
import org.apache.http.entity.HttpEntityWrapper;

import com.google.common.io.ByteStreams;

/** An implementation of a {@link HttpEntityFactory} that returns an entity whose content is buffered using a {@link FastByteArrayInputStream}. */
public class FastByteArrayInputStreamHttpEntityFactory implements HttpEntityFactory {

	protected FastByteArrayOutputStream cachedContent = new FastByteArrayOutputStream();

	@Override
	public HttpEntity newEntity(final HttpEntity from) throws IOException {

		return new HttpEntityWrapper(from) {

			private FastByteArrayInputStream content = null;

			@Override
			public InputStream getContent() throws IOException {
				if (content != null) return content;
				final InputStream in = wrappedEntity.getContent();
				try {
					ByteStreams.copy(in, cachedContent);
				} finally {
					try { in.close(); } catch (Exception ignore) {}
				}
				content = new FastByteArrayInputStream(cachedContent.array, 0, cachedContent.length);
				content.position(0);
				return content;
			}

			@Override
			public long getContentLength() {
				if (content == null) throw new IllegalStateException();
				return cachedContent.length;
			}

		};

	}

}
