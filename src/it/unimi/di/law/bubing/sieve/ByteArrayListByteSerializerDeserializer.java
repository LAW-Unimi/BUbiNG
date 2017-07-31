package it.unimi.di.law.bubing.sieve;

/*
 * Copyright (C) 2010-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
