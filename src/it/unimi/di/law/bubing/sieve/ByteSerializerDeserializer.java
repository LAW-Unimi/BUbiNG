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
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

//RELEASE-STATUS: DIST

/** A light for of serialization based on {@link InputStream}/{@link OutputStream} (and
 * {@link FastBufferedInputStream} for fast skipping). */
public interface ByteSerializerDeserializer<V> {

	/** A NOP-serializer-deserializer for Void (only for values). */
	public final static ByteSerializerDeserializer<Void> VOID = new ByteSerializerDeserializer<Void>() {
		@Override
		public Void fromStream(final InputStream is) throws IOException {
			return null;
		}
		@Override
		public void toStream(final Void v, final OutputStream os) throws IOException {}
		@Override
		public void skip(final FastBufferedInputStream is) throws IOException {}
	};

	/** A trivial serializer-deserializer for {@link Integer}. */
	public final static ByteSerializerDeserializer<Integer> INTEGER = new ByteSerializerDeserializer<Integer>() {
		@Override
		public Integer fromStream(final InputStream is) throws IOException {
			return Integer.valueOf(new DataInputStream(is).readInt());
		}
		@Override
		public void toStream(final Integer i, final OutputStream os) throws IOException {
			new DataOutputStream(os).writeInt(i.intValue());
		}
		@Override
		public void skip(final FastBufferedInputStream is) throws IOException {
			is.skip(4);
		}
	};

	/** A serializer-deserializer for byte arrays that write the array length using variable-length byte encoding,
	 * and the writes the content of the array. */
	public final static ByteSerializerDeserializer<byte[]> BYTE_ARRAY = new ByteSerializerDeserializer<byte[]>() {
		@Override
		public byte[] fromStream(final InputStream is) throws IOException {
			final int length = Util.readVByte(is);
			final byte[] array = new byte[length];
			final int actual = is.read(array);
			if (actual != length) throw new IOException("Asked for " + length + " but got " + actual);
			return array;
		}
		@Override
		public void toStream(final byte[] array, final OutputStream os) throws IOException {
			final int length = array.length;

			Util.writeVByte(length, os);
			os.write(array);
		}
		@Override
		public void skip(final FastBufferedInputStream is) throws IOException {
			int length = 0, b;

			while((b = is.read()) >= 0x80) {
				length |= b & 0x7F;
				length <<= 7;
			}
			if (b == -1) throw new EOFException();
			length |= b;

			final long actual = is.skip(length);
			if (actual != length) throw new IOException("Asked for " + length + " but got " + actual);
		}
	};

	/** Deserializes an object starting from a given portion of a byte array.
	 *
	 * @param is the input stream from which the object will be deserialized.
	 * @return a new object, deserialized from the given portion of the array.
	 */
	public V fromStream(InputStream is) throws IOException;

	/** Serializes an object starting from a given offset of a byte array.
	 *
	 * @param v the object to be serialized.
	 * @param os the output stream that will receive the serialized object.
	 */
	public void toStream(V v, OutputStream os) throws IOException;

	/** Skip an object, usually without deserializing it.
	 *
	 * <p>Note that this method
	 * <em>requires explicitly a {@link FastBufferedInputStream}</em>. As
	 * a result, you can safely use {@link FastBufferedInputStream#skip(long) skip()} to
	 * skip the number of bytes required (see the documentation of {@link FastBufferedInputStream#skip(long)}
	 * for some elaboration).
	 *
	 * <p>Calling this method must be equivalent to calling {@link #fromStream(InputStream)}
	 * and discarding the result.
	 *
	 * @param is the fast buffered input stream from which the next object will be skipped.
	 */
	public void skip(FastBufferedInputStream is) throws IOException;
}
