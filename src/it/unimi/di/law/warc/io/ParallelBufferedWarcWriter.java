package it.unimi.di.law.warc.io;

/*
 * Copyright (C) 2004-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;

/** A parallel Warc writer. Instances of this class use a number of internal buffer and {@link WarcWriter} instances
 * to parallelize record compression.
 *
 * <p>Records will be written to the {@link OutputStream} provided at construction time (which we suggest to buffer heavily)
 * by a flushing thread. There is no guarantee about the order in which the records will be output.
 *
 * <p>Note that for each thread there is an associated {@link FastByteArrayOutputStream} that will grow as needed to accommodate
 * the output of the record.
 */
public class ParallelBufferedWarcWriter implements WarcWriter {

	/** The queue of empty {@link ParallelBufferedWarcWriter.WriterPair} instances. */
	protected final ArrayBlockingQueue<WriterPair> emptyPairs;

	/** The queue of filled {@link ParallelBufferedWarcWriter.WriterPair} instances; their content will be flushed to disk by the {@link #flushingThread}. */
	protected final ArrayBlockingQueue<WriterPair> filledPairs;

	/** The thread that iteratively extracts filled @link ParallelBufferedWarcWriter.WriterPair} instances from {@link #filledPairs},
	 * dump them to {@link #outputStream} and enqueue them to {@link #emptyPairs}. */
	protected final FlushingThread flushingThread;

	/** The final output stream. */
	protected final OutputStream outputStream;

	/** The exception throw by the {@link #flushingThread}, if any, or {@code null}. */
	protected volatile IOException flushingThreadException;

	protected static final class WriterPair implements WarcWriter {
		private final WarcWriter writer;
		private final FastByteArrayOutputStream stream;
		private WriterPair(final WarcWriter writer, final FastByteArrayOutputStream stream) {
			this.writer = writer;
			this.stream = stream;
		}
		@Override
		public void close() throws IOException {
			writer.close();
		}
		@Override
		public void write(final WarcRecord record) throws IOException, InterruptedException {
			writer.write(record);
		}
	}

	private final class FlushingThread extends Thread {
		@Override
		public void run() {
			WriterPair pair;
			try {
				while (! Thread.currentThread().isInterrupted()) {
					pair = filledPairs.take();
					try {
						outputStream.write(pair.stream.array, 0, pair.stream.length);
					}
					catch (Exception e) {
						flushingThreadException = e instanceof IOException ? (IOException)e : new IOException(e);
						return;
					}
					emptyPairs.add(pair);
				}


			} catch (InterruptedException exit) {}
			finally {
				while((pair = filledPairs.poll()) != null) {
					try {
						outputStream.write(pair.stream.array, 0, pair.stream.length);
					}
					catch (Exception e) {
						flushingThreadException = e instanceof IOException ? (IOException)e : new IOException(e);
						return;
					}
				}
			}
		}
	}

	/** Creates a Warc parallel output stream using 2&times;{@link Runtime#availableProcessors()} buffers.
	 *
	 * @param outputStream the final output stream.
	 * @param compress whether to write compressed records.
	 */
	public ParallelBufferedWarcWriter(final OutputStream outputStream, final boolean compress) {
		this(outputStream, compress, 2 * Util.RUNTIME.availableProcessors());
	}

	/** Creates a Warc parallel output stream.
	 *
	 * @param outputStream the final output stream.
	 * @param compress whether to write compressed records.
	 * @param numberOfBuffers the number of buffers.
	 */
	public ParallelBufferedWarcWriter(final OutputStream outputStream, final boolean compress, final int numberOfBuffers) {
		this.outputStream = outputStream;
		emptyPairs = new ArrayBlockingQueue<>(numberOfBuffers);
		filledPairs = new ArrayBlockingQueue<>(numberOfBuffers);
		for(int i = numberOfBuffers; i-- != 0;) {
			final FastByteArrayOutputStream stream = new FastByteArrayOutputStream();
			emptyPairs.add(new WriterPair(compress ? new CompressedWarcWriter(stream) : new UncompressedWarcWriter(stream), stream));
		}
		(flushingThread = new FlushingThread()).start();
		flushingThread.setName(ParallelBufferedWarcWriter.class.getSimpleName());
	}

	@Override
	public void write(WarcRecord record) throws IOException, InterruptedException {
		final WriterPair pair = emptyPairs.take();
		pair.stream.reset();
		pair.writer.write(record);
		filledPairs.add(pair);
	}

	@Override
	public synchronized void close() throws IOException {
		if (flushingThreadException != null) throw flushingThreadException;
		flushingThread.interrupt();
		try {
			flushingThread.join();
		}
		catch (InterruptedException shouldntHappen) {
			throw new IOException("Interrupted while joining flushing thread");
		}
		outputStream.close();
		emptyPairs.clear();
		filledPairs.clear();
	}
}
