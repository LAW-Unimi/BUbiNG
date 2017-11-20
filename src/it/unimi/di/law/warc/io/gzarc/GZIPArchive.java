package it.unimi.di.law.warc.io.gzarc;

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

import it.unimi.di.law.bubing.util.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrays;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;

/**
 * A <em>GZIP archive</em> is an archive made of (concatenated) GZIP entries that are usual GZIP files, except for the
 * presence of two extra fields (in the GZIP header) containing the compressed and uncompressed length of the entry itself.
 *
 * This class contains the definition of {@link GZIPArchive.ReadEntry} and {@link GZIPArchive.WriteEntry} that are used by
 * {@link GZIPArchiveReader} to read, or respectively by {@link GZIPArchiveWriter} to write, entries of a GZIP archive.
 *
 */
public class GZIPArchive {

	/**
	 * A generic GZIP archive entry; it can be instantiated only as a  {@link GZIPArchive.ReadEntry} or  {@link GZIPArchive.WriteEntry}.
	 */
	public static class Entry {

		/** The actual (compressed) length of the entry. */
		public int compressedSkipLength;

		/** The length of the entry one uncompressed. */
		public int uncompressedSkipLength;

		/** The modification time of the entry. */
		public int mtime;

		/** The CRC of the entry. */
		public int crc32;

		/** An internal representation of the name of the entry. */
		protected byte[] name = ByteArrays.EMPTY_ARRAY;

		/** An internal representation of the comment of the entry. */
		protected byte[] comment = ByteArrays.EMPTY_ARRAY;

		private Entry() {}

		/** Returns the name of the entry.
		 *
		 * @return the name of the entry.
		 */
		public String getName() {
			return Util.toString(this.name);
		}

		/** Sets the name of the entry.
		 *
		 * @param name the name of the entry.
		 */
		public void setName(final String name) {
			this.name = Util.toByteArray(name);
		}

		/** Returns the comment of the entry.
		 *
		 * @return the comment of the entry.
		 */
		public String getComment() {
			return Util.toString(this.comment);
		}

		/** Sets the comment of the entry.
		 *
		 * @param comment the comment of the entry.
		 */
		public void setComment(final String comment) {
			this.comment = Util.toByteArray(comment);
		}

		@Override
		public String toString() {
			return "<" + getName() + ", " + getComment() + ", csl = " + compressedSkipLength + ", usl = " + uncompressedSkipLength + ", mtime = " + mtime + ", crc32 = " + (crc32 != 0 ? Integer.valueOf(crc32) : "[not checked]") + ">";
		}
	}

	/**
	 * An entry used to read a GZIP archive entry.
	 *
	 * In order to get the entry content one must first call the {@link LazyInflater#get()} method to get the {@link InputStream}
	 * to read from; once (part of) such stream has been read, one must call the {@link LazyInflater#consume()} method before any
	 * subsequent read from the same archive.
	 */
	public static class ReadEntry extends Entry {
	    	public LazyInflater lazyInflater = null;
		/** The lazy infalter that can be used to get (part of the) uncompressed entry content. */
		public interface LazyInflater {
			/** Returns the actual inflater from which the uncompressed entry content may be read. If cached, can be called multimple times per entry. */
			public InputStream get() throws IOException;
			/** Consumes the (possibly) remaining entry content. Must be called exactly once per entry. */
			public void consume() throws IOException;
		}
	}

	/**
	 * An entry used to write a GZIP archive entry.
	 *
	 * In order to write the entry content write it to the {@link #deflater}.
	 */
	public static class WriteEntry extends Entry {
		/** The deflater to be used to actually write data that has to be compressed as the entry content. */
		public OutputStream deflater = null;
	}

	@SuppressWarnings("serial")
	public static class FormatException extends IOException {

		public FormatException(String message) {
			super(message);
		}

		public FormatException(String message, Throwable e) {
			super(message, e);
		}
	}

	public static final int CHECKSUM_THRESHOLD = 1024;

	/* GZIP constants */

	public static final byte XFL = Deflater.BEST_COMPRESSION;
	public static final byte FTEXT = 1 << 0, FHCRC = 1 << 1, FEXTRA = 1 << 2, FNAME = 1 << 3, FCOMMENT = 1 << 4;
	public static final byte[]	GZIP_START = new byte[] {
		(byte)0x1F, (byte)0x8B,		// ID1 ID2
		Deflater.DEFLATED,			// CM
		FEXTRA | FNAME | FCOMMENT	// FLG
	};
	public static final byte[] XFL_OS = new byte[] {
		XFL,
		(byte)0xFF // unknown os
	};
	public static final short SHORT_LEN = 2;
	public static final short INT_LEN = 4;
	public static final byte[] SKIP_LEN = new byte[] { (byte)'s', (byte)'l' };
	public static final short SUB_LEN = 2 * INT_LEN; //  compressedSkipLength, uncompressedSkipLength
	public static final short XLEN = (short)(SKIP_LEN.length + SHORT_LEN + SUB_LEN); // the sort serve to encode SUB_LEN
	public static final short TRAILER_LEN = 2 * INT_LEN; // CRC32, ISIZE
	public static final int FIX_LEN = GZIP_START.length +
										INT_LEN + // MTIME
										XFL_OS.length +
										SHORT_LEN + XLEN + // the short ennodes XLEN
										TRAILER_LEN;

}
