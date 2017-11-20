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

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.lang.MutableString;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;

//RELEASE-STATUS: DIST

/** An example of {@link ByteSerializerDeserializer}. Not a very efficient one; it is intended only as an example of how a class of this kind works.
 */
public class CharSequenceByteSerializerDeserializer implements ByteSerializerDeserializer<CharSequence> {

	private static final CharSequenceByteSerializerDeserializer INSTANCE = new CharSequenceByteSerializerDeserializer();

	protected CharSequenceByteSerializerDeserializer() {}

	public static CharSequenceByteSerializerDeserializer getInstance() {
		return INSTANCE;
	}

	@Override
	public void toStream(final CharSequence src, final OutputStream os) throws IOException {
		new MutableString(src).writeSelfDelimUTF8(os);
	}

	@Override
	public CharSequence fromStream(final InputStream is) throws IOException {
		return new MutableString().readSelfDelimUTF8(is);
	}

	@Override
	public void skip(final FastBufferedInputStream is) throws IOException {
		// Borrowed from it.unimi.dsi.lang.MutableString.
		int length = 0, b;

		for(;;) {
			if ((b = is.read()) < 0) throw new EOFException();
			if ((b & 0x80) == 0) break;
			length |= b & 0x7F;
			length <<= 7;
		}
		length |= b;

		for(int i = 0; i < length; i++) {
			b = is.read() & 0xFF;
			switch (b >> 4) {
			case 0:
			case 1:
			case 2:
			case 3:
			case 4:
			case 5:
			case 6:
			case 7:
				break;
			case 12:
			case 13:
				is.skip(1);
				break;
			case 14:
				is.skip(2);
				break;
			default:
				throw new UTFDataFormatException();
			}
		}
	}
}
