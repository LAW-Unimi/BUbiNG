package it.unimi.di.law.warc.util;

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
