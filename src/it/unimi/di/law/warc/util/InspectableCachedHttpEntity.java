package it.unimi.di.law.warc.util;

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

import it.unimi.di.law.bubing.util.TooSlowException;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.http.HttpEntity;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.HttpEntityWrapper;

/** An implementation of a {@link HttpEntity} that is reusable and can copy its content from another entity at a controlled rate.
 *
 * This entity store its content in a {@link InspectableFileCachedInputStream} and can copy the content from another entity
 * using {@link #copyContent(long, long, long, long)} possibly throwing a {@link TooSlowException} in case the copy speed gets
 * to slow.
 *
 */
public class InspectableCachedHttpEntity extends HttpEntityWrapper {
	private static final int BUFFER_SIZE = 8192;
	private final static HttpEntity THROW_AWAY_ENTITY = new BasicHttpEntity();

	private final InspectableFileCachedInputStream cachedContent;
	private final byte[] buffer;
	private final ByteBuffer byteBuffer;

	public InspectableCachedHttpEntity(final InspectableFileCachedInputStream cachedContent) {
		super(THROW_AWAY_ENTITY);
		this.cachedContent = cachedContent;
		buffer = new byte[BUFFER_SIZE];
		byteBuffer = ByteBuffer.wrap(buffer);
	}

	public void setEntity(final HttpEntity wrappedEntity) throws IOException {
		this.wrappedEntity = wrappedEntity;
		this.cachedContent.clear();
	}

	public boolean copyContent(final long maxLength, final long startTime, final long minDelay, final long minBytesPerSecond) throws IOException, TooSlowException {
		if (this.wrappedEntity == THROW_AWAY_ENTITY) throw new IllegalStateException();
		final InputStream content = this.wrappedEntity.getContent();

		if (maxLength != 0) {
			long count = 0;
			for (int r; ((r = content.read(buffer, 0, (int)Math.min(BUFFER_SIZE, maxLength - count)))) != -1;) {
				byteBuffer.clear().limit(r);
				cachedContent.write(byteBuffer);
				count += r;
				if (count == maxLength) break;
				final long delay = System.currentTimeMillis() - startTime;
				final double bytesPerSecond = count / (delay / 1000.);
				if (delay > minDelay && bytesPerSecond < minBytesPerSecond) throw new TooSlowException(bytesPerSecond + " B/s");
			}
		}

        return content.read(buffer, 0, 1) != -1;
	}


	public void clear() throws IOException {
		this.cachedContent.clear();
	}

	@Override
	public InputStream getContent() throws IOException {
		if (cachedContent == null) throw new IllegalStateException();
		cachedContent.reopen();
		return cachedContent;
	}

	@Override
	public long getContentLength() {
		if (cachedContent == null) throw new IllegalStateException();
		long length = -1;
		try {
			length = cachedContent.length();
		} catch (IOException ignored) {}
		return length;
	}
}
