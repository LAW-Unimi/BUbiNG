package it.unimi.di.law.bubing.sieve;

/*
 * Copyright (C) 2010-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.bubing.util.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

//RELEASE-STATUS: DIST

/** A {@link ByteSerializerDeserializer} based on {@link ByteArrayList}. */

public class ByteArrayListByteSerializerDeserializer implements ByteSerializerDeserializer<ByteArrayList> {
	private final ByteArrayList buffer = new ByteArrayList();

	/** A serializer-deserializer for byte arrays that write the array length using variable-length byte encoding,
	 * and the writes the content of the array. */
	@Override
	public ByteArrayList fromStream(final InputStream is) throws IOException {
		final int length = Util.readVByte(is);
		buffer.size(length);
		final int actual = is.read(buffer.elements(), 0, length);
		if (actual != length) throw new IOException("Asked for " + length + " but got " + actual);
		return buffer;
	}
	@Override

	public void toStream(final ByteArrayList list, final OutputStream os) throws IOException {
		final int length = list.size();
		Util.writeVByte(length, os);
		os.write(list.elements(), 0, length);
	}

	@Override
	public void skip(final FastBufferedInputStream is) throws IOException {
		final int length = Util.readVByte(is);
		final long actual = is.skip(length);
		if (actual != length) throw new IOException("Asked for " + length + " but got " + actual);
	}
}
