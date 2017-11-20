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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.RepositionableStream;

// RELEASE-STATUS: DIST

public class GZIPArchiveReader {

	private final static Logger LOGGER = LoggerFactory.getLogger(GZIPArchiveReader.class);

	private final CRC32 crc = new CRC32();

	private static final int HEADER_BUFFER_SIZE = 16384;
	private final byte[] headerBuffer = new byte[HEADER_BUFFER_SIZE];

	private final CountingInputStream input;
	private final RepositionableStream repositionableInput;

	public GZIPArchiveReader(final InputStream input) {
		this.input = new CountingInputStream(input);
		this.repositionableInput = input instanceof RepositionableStream ? (RepositionableStream)input : null;
	}

	public void position(long position) throws IOException {
		if (this.repositionableInput == null) throw new UnsupportedOperationException();
		this.repositionableInput.position(position);
	}

	public GZIPArchive.ReadEntry getEntry() throws IOException {
		return getEntry(false);
	}

	public GZIPArchive.ReadEntry skipEntry() throws IOException {

		final long headerCount = input.getCount();
		final GZIPArchive.ReadEntry entry = readHeader();
		final long compressedBlockCount = input.getCount();
		if (entry == null) return null;

		final long reminingCompressedBytes = entry.compressedSkipLength - (compressedBlockCount - headerCount) - GZIPArchive.TRAILER_LEN;
		input.skip(reminingCompressedBytes);
		readTrailer(entry);

		return entry;
	}

	public GZIPArchive.ReadEntry getEntry(final boolean cached) throws IOException {

		final long headerCount = input.getCount();
		final GZIPArchive.ReadEntry entry = readHeader();
		final long compressedBlockCount = input.getCount();
		if (entry == null) return null;

		final long reminingCompressedBytes = entry.compressedSkipLength - (compressedBlockCount - headerCount) - GZIPArchive.TRAILER_LEN;
		final InputStream limitInputStream = ByteStreams.limit(input, reminingCompressedBytes);
		final InputStream boundingInputStream = cached ? new FastByteArrayInputStream(ByteStreams.toByteArray(limitInputStream)) : limitInputStream;

		entry.lazyInflater = new GZIPArchive.ReadEntry.LazyInflater() {

			private InputStream inflaterInputStream;

			@Override
			public void consume() throws IOException  {

				// This must be called after get!

				if (inflaterInputStream == null && ! cached) throw new IllegalStateException("Can't call 'consume' before 'get' if not cached");

				// Consume the possibly not yet read compressed bytes

				boolean uncompress = input.getCount() - compressedBlockCount < GZIPArchive.CHECKSUM_THRESHOLD;
				if (uncompress && ! cached) {
					long read = inflaterInputStream.skip(Long.MAX_VALUE);
					if (LOGGER.isTraceEnabled()) LOGGER.trace("Read remaining {} uncompressed bytes", Long.valueOf(read));
				} else {
					long read = limitInputStream.skip(Long.MAX_VALUE);
					if (LOGGER.isTraceEnabled()) LOGGER.trace("Read remaining {} compressed bytes", Long.valueOf(read));
				}

				assert input.getCount() - compressedBlockCount == reminingCompressedBytes : "Wrong reminingCompressedBytes after consuming content.";

				// Read and check trailer

				readTrailer(entry);

				if (uncompress && ! cached) {
					final int actualCrc = (int)(crc.getValue() & 0xFFFFFFFF);
					if (LOGGER.isTraceEnabled()) LOGGER.trace("CRC as computed while reading {}", Integer.valueOf(actualCrc));
					if (entry.crc32 != actualCrc) throw new GZIPArchive.FormatException("CRC32 mismatch, expected: " + entry.crc32 + ", actual: " + actualCrc);
				} else entry.crc32 = 0;

				// We can't close any involved stream since it will popup to 'input'

				assert ! cached || reminingCompressedBytes == ((FastByteArrayInputStream)boundingInputStream).available() : "Wrong reminingCompressedBytes after reading the trailer";

			}

			@Override
			public InputStream get() throws IOException {
				Inflater inflater = new Inflater(true);
				crc.reset();

				if (cached) ((FastByteArrayInputStream)boundingInputStream).position(0); // TODO: see if we can cache the decompressed stream

				inflaterInputStream = cached ?
					new InflaterInputStream(boundingInputStream, inflater) :
					new CheckedInputStream(new InflaterInputStream(boundingInputStream, inflater), crc);

				assert ! cached || reminingCompressedBytes == ((FastByteArrayInputStream)boundingInputStream).available() : "Wrong reminingCompressedBytes count after get";

				return inflaterInputStream;

			}

		};

		return entry;

	}

	/** Reads the GZIP entry trailer (populating the entry CRC metadata).
	 *
	 * @param entry the entry where to store the CRC of the entry.
	 *
	 */
	private void readTrailer(final GZIPArchive.ReadEntry entry) throws IOException {

		// CRC32

		entry.crc32 = readLEInt(input);
		if (LOGGER.isTraceEnabled()) LOGGER.trace("CRC read from stream {}", Integer.valueOf(entry.crc32));

		// ISIZE

		final int iSize = readLEInt(input);
		if (entry.uncompressedSkipLength != iSize) throw new GZIPArchive.FormatException("Length mismatch between (warc) extra gzip fields uncompressed-skip-length (" + entry.uncompressedSkipLength + ") and ISIZE (" + iSize + ")");

	}

	/** Reads the GZIP entry header (popluating the entry metadata, except for the CRC).
	 *
	 * @param entry the entry where to store the entry metadata (except for the CRC that is read with the trailer).
	 * @return the number of bytes consumed while reading the header, or -1 in case of EOF.
	 */
	private GZIPArchive.ReadEntry readHeader() throws IOException {

		final GZIPArchive.ReadEntry entry = new GZIPArchive.ReadEntry();
		byte[] buffer = headerBuffer; // local copy for efficiency reasons

		// ID1 ID2 CM FLG

		if (input.read(buffer, 0, 4) == -1) return null;

		if (buffer[0] != GZIPArchive.GZIP_START[0] || buffer[1] != GZIPArchive.GZIP_START[1]) throw new GZIPArchive.FormatException("Missing GZip magic numbers, found: " + buffer[0] + " " + buffer[1]);
		if (buffer[2] != GZIPArchive.GZIP_START[2]) throw new GZIPArchive.FormatException("Unknown compression method: " + buffer[2]);
		int flg = buffer[3];

		// MTIME

		entry.mtime = readLEInt(input);

		// XFL OS (ignored)

		this.input.read(buffer, 0, 2);

		/* EXTRA begin */

		entry.compressedSkipLength = -1;

		if ((flg & GZIPArchive.FEXTRA) != 0) {

			// XLEN

			short xlen = readLEShort(input);

			while (xlen > 0) {

				// SI1 SI2

				input.read(buffer, 0, 2);

				// LEN

				short len = readLEShort(input);

				if (buffer[0] == GZIPArchive.SKIP_LEN[0] && buffer[1] == GZIPArchive.SKIP_LEN[1]) {
					entry.compressedSkipLength = readLEInt(input);
					entry.uncompressedSkipLength = readLEInt(input);
				} else input.read(buffer, 0, len);

				xlen -= len + GZIPArchive.SKIP_LEN.length + GZIPArchive.SHORT_LEN; // SI1,  SI2 + 1 short encoding LEN

			}
		} else throw new GZIPArchive.FormatException("Missing SL extra field");

		if (entry.compressedSkipLength < 0) throw new GZIPArchive.FormatException("Negative compressed-skip-length (" + entry.compressedSkipLength + ")");

		/* EXTRA end */

		// NAME

		if ((flg & GZIPArchive.FNAME) != 0) {
			int len = 0, b;
			while ((b = this.input.read()) != 0) {
				 buffer[len++] = (byte)b;
			}
			entry.name = Arrays.copyOf(buffer, len);
		}

		// COMMENT

		if ((flg & GZIPArchive.FCOMMENT) != 0) {
			int len = 0, b;
			while ((b = this.input.read()) != 0) {
				 buffer[len++] = (byte)b;
			}
			entry.comment = Arrays.copyOf(buffer, len);
		}

		// HCRC

		if ((flg & GZIPArchive.FHCRC) != 0) {
			this.input.read(buffer, 0, 2);
		}

		return entry;
	}

	/** Reads an integer in little-endian from the stream. */
	private static int readLEInt(InputStream in) throws IOException {
		int i = in.read() & 0xFF;
		i |= (in.read() & 0xFF) << 8;
		i |= (in.read() & 0xFF) << 16;
		i |= (in.read() & 0xFF) << 24;
		return i;
	}

	/** Reads a short in little-endian from the stream. */
	private static short readLEShort(InputStream in) throws IOException {
		short s = (byte)in.read();
		s |= (byte)in.read() << 8;
		return s;
	}
}

