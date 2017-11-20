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

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

// This is ThreadSafe!
public class GZIPArchiveWriter implements Closeable {

	private final CRC32 crc = new CRC32();
	private final Deflater deflater = new Deflater(GZIPArchive.XFL, true);
	private final FastByteArrayOutputStream deflaterStream = new FastByteArrayOutputStream();
	private final OutputStream output;

	public GZIPArchiveWriter(final OutputStream output) {
		this.output = output;
	}

	@Override
	public void close() throws IOException {
		this.output.close();
	}

	/**
	 * Writes the entry on the underlying stream.
	 *
	 * More precisely, it writes the GZIP header, the content of the (bufferized) deflater stream and then the CZIP trailer.
	 */
	protected void writeEntry(final GZIPArchive.Entry entry) throws IOException {

		// ID1 ID2 CM FLG

		this.output.write(GZIPArchive.GZIP_START);

		// MTIME

		writeLEInt(this.output, entry.mtime);

		// XFL OS

		this.output.write(GZIPArchive.XFL_OS);

		/* EXTRA begin */

		// XLEN

		writeLEShort(this.output, GZIPArchive.XLEN);

		// SI1 SI2 (as in warc spec)

		this.output.write(GZIPArchive.SKIP_LEN);

		// LEN

		writeLEShort(this.output, GZIPArchive.SUB_LEN);

		// compressed-skip-length (as in warc spec)

		writeLEInt(this.output, entry.compressedSkipLength);

		// uncompressed length (as in warc spec)

		writeLEInt(this.output, entry.uncompressedSkipLength);

		/* EXTRA end */

		// NAME

		this.output.write(entry.name);
		this.output.write(0);

		// COMMENT

		this.output.write(entry.comment);
		this.output.write(0);

		// compressed blocks

		this.output.write(deflaterStream.array, 0, deflaterStream.length);

		// CRC32

		writeLEInt(this.output, entry.crc32);

		// ISIZE

		writeLEInt(this.output, entry.uncompressedSkipLength);

	}

	/**
	 * Returns an object that can be used to write an entry in the GZIP archive.
	 *
	 * In order to write the actual entry, one must write the entry content on the {@link GZIPArchive.WriteEntry#deflater} and,
	 * at the end, call its <code>close()</code> method (to actually write the compressed content).
	 *
	 * @param name the name of the entry.
	 * @param comment the comment of the entry.
	 * @param creationDate the date in which the entry has been created.
	 *
	 */
	public GZIPArchive.WriteEntry getEntry(final String name, final String comment, final Date creationDate) {

		crc.reset();
		deflater.reset();
		deflaterStream.reset();

		final GZIPArchive.WriteEntry entry = new GZIPArchive.WriteEntry();
		entry.setName(name);
		entry.setComment(comment);
		entry.deflater = new FilterOutputStream(new DeflaterOutputStream(deflaterStream, deflater)) {
			private final byte[] oneCharBuffer = new byte[1];
			private long length = 0;

			@Override
			public void write(int b) throws IOException {
				// This avoids byte-array creation in DeflaterOutputStream.write()
				oneCharBuffer[0] = (byte)b;
				this.out.write(oneCharBuffer);
				crc.update(oneCharBuffer);
				this.length++;
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				this.out.write(b, off, len);
				crc.update(b, off, len);
				this.length += len;
			}

			@Override
			public void close() throws IOException {
				this.out.flush();
				((DeflaterOutputStream)this.out).finish();

				entry.compressedSkipLength = GZIPArchive.FIX_LEN + (entry.name.length + 1) + (entry.comment.length + 1) + deflaterStream.length;
				entry.uncompressedSkipLength = (int)(this.length & 0xFFFFFFFF);
				entry.mtime = (int)(creationDate.getTime() / 1000);
				entry.crc32 = (int)(crc.getValue() & 0xFFFFFFFF);

				writeEntry(entry);
			}
		};

		return entry;
	}

	/** Writes the given integer in little-endian on the stream. */
	private static void writeLEInt(OutputStream out, int i) throws IOException {
		out.write((byte)i);
		out.write((byte)((i >> 8) & 0xFF));
		out.write((byte)((i >> 16) & 0xFF));
		out.write((byte)((i >> 24) & 0xFF));
	}

	/** Writes the given short in little-endian on the stream. */
	private static void writeLEShort(OutputStream out, short s) throws IOException {
		out.write((byte)s);
		out.write((byte)((s >> 8) & 0xFF));
	}

}
