package it.unimi.di.law.bubing.util;

/*
 * Copyright (C) 2012-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.io.ByteDiskQueue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

//RELEASE-STATUS: DIST

/** A queue of byte arrays partially stored on disk.
 *
 * <P>This class is a wrapper around a {@link ByteDiskQueue} that provides methods to
 * {@linkplain #enqueue(byte[]) enqueue} and {@linkplain #dequeue()} byte arrays. Like a
 * {@link ByteDiskQueue}, and instance of this class has a {@link #suspend()} method that releases
 * the file handle without deleting the dump file. The file handle is reinitialized lazily (which
 * implies that a certain number of {@link #enqueue(byte[])}/{@link #dequeue()} calls can be performed on a suspended
 * class without actually reopening the dump file).
 *
 * <p>Similarly to a {@link ByteDiskQueue}, you can {@link #freeze()} and then
 * {@linkplain #createFromFile(long, File, int, boolean) reopen} a {@link ByteArrayDiskQueue}.
 *
 * @see ByteDiskQueue
 */

public class ByteArrayDiskQueue implements Closeable, Size64 {

	/** The underlying byte disk queue. */
	private final ByteDiskQueue byteDiskQueue;
	/** The number of byte arrays in this queue. */
	private long size;
	/** The buffer used to hold the last {@link #dequeue() dequeued} byte array. */
	private ByteArrayList buffer;

	protected ByteArrayDiskQueue(ByteDiskQueue byteDiskQueue) {
		this.byteDiskQueue = byteDiskQueue;
		this.buffer = new ByteArrayList(1024);
	}

	/** Creates a new disk-based queue of byte arrays.
	 *
	 * @param file the file that will be used to dump the queue on disk.
	 * @param bufferSize the number of items in the circular buffer (will be possibly decreased so to be a power of two).
	 * @param direct whether the {@link ByteBuffer} used by this queue should be {@linkplain ByteBuffer#allocateDirect(int) allocated directly}.
	 * @see ByteDiskQueue#createNew(File, int, boolean)
	 */
	public static ByteArrayDiskQueue createNew(final File file, final int bufferSize, final boolean direct) throws IOException {
		return new ByteArrayDiskQueue(ByteDiskQueue.createNew(file,  bufferSize, direct));
	}

	/** Creates a new disk-based queue of byte arrays using an existing file.
	 *
	 * <p>Note that you have to supply the correct number of byte arrays contained in the dump file of
	 * the underlying {@link ByteDiskQueue}. Failure to do so will cause unpredictable behaviour.
	 *
	 * @param size the number of byte arrays contained in {@code file}.
	 * @param file the file that will be used to dump the queue on disk.
	 * @param bufferSize the number of items in the circular buffer (will be possibly decreased so to be a power of two).
	 * @param direct whether the {@link ByteBuffer} used by this queue should be {@linkplain ByteBuffer#allocateDirect(int) allocated directly}.
	 * @see ByteDiskQueue#createFromFile(File, int, boolean)
	 */
	public static ByteArrayDiskQueue createFromFile(final long size, final File file, final int bufferSize, final boolean direct) throws IOException {
		final ByteArrayDiskQueue byteArrayDiskQueue = new ByteArrayDiskQueue(ByteDiskQueue.createFromFile(file, bufferSize, direct));
		byteArrayDiskQueue.size = size;
		return byteArrayDiskQueue;
	}

	/** Enqueues a byte array to this queue.
	 *
	 * @param array the array to be enqueued.
	 */
	public synchronized void enqueue(final byte[] array) throws IOException {
		assert array != null;
		byteDiskQueue.enqueueInt(array.length);
		byteDiskQueue.enqueue(array);
		size++;
	}

	/** Enqueues a byte-array fragment to this queue.
	 *
	 * @param array a byte array.
	 * @param offset the first valid byte in {@code array}.
	 * @param length the number of valid elements in {@code array}.
	 */
	public synchronized void enqueue(final byte[] array, final int offset, final int length) throws IOException {
		assert array != null;
		byteDiskQueue.enqueueInt(length);
		byteDiskQueue.enqueue(array, offset, length);
		size++;
	}

	/** Dequeues a byte array from the queue in FIFO fashion. The actual byte array
	 * will be stored in the {@linkplain #buffer() queue buffer}. */
	public synchronized void dequeue() throws IOException {
		final int length = byteDiskQueue.dequeueInt();
		buffer.size(length);
		byteDiskQueue.dequeue(buffer.elements(), 0, length);
		size--;
	}

	/** Returns the current buffer of this byte-array disk queue.
	 * The last {@link #dequeue()} has filled this buffer with the result of the dequeue operation.
	 * Only the first <var>k</var> bytes, where <var>k</var> is the number returned by {@link #dequeue()}, are valid.
	 *
	 * @return the current buffer of this byte-array disk queue.
	 */
	public ByteArrayList buffer() {
		return buffer;
	}

	@Override
	@Deprecated
	public synchronized int size() {
		throw new UnsupportedOperationException();
	}

	@Override
	public synchronized long size64() {
		return size;
	}

	public synchronized boolean isEmpty() {
		return size == 0;
	}

	/** Closes this queue.
	 *
	 * @see ByteDiskQueue#close()
	 */
	@Override
	public synchronized void close() throws IOException {
		byteDiskQueue.close();
	}

	/** Freezes this queue.
	 *
	 * @see ByteDiskQueue#freeze()
	 */
	public synchronized void freeze() throws IOException {
		byteDiskQueue.freeze();
	}

	/** Clears this queue.
	 *
	 * @see ByteDiskQueue#clear()
	 */
	public synchronized void clear() {
		byteDiskQueue.clear();
		size = 0;
	}

	/** Trims this queue.
	 *
	 * @see ByteDiskQueue#trim()
	 */
	public synchronized void trim() throws IOException {
		byteDiskQueue.trim();
	}

	/** Suspends this queue.
	 * @see ByteDiskQueue#suspend()
	 */
	public synchronized void suspend() throws IOException {
		byteDiskQueue.suspend();
	}

	/** Enlarge the buffer of this queue to a given size.
	 *
	 * @param newBufferSize the required buffer size.
	 * @see ByteDiskQueue#enlargeBuffer(int)
	 */
	public synchronized void enlargeBuffer(final int newBufferSize) {
		byteDiskQueue.enlargeBuffer(newBufferSize);
	}
}
