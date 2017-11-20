package it.unimi.di.law.bubing.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

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
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.ByteDiskQueue;

//RELEASE-STATUS: DIST

/** A queue of objects partially stored on disk.
 *
 * <P>This class is a wrapper around a {@link ByteDiskQueue} that provides methods to
 * {@linkplain #enqueue(Object) enqueue} and {@linkplain #dequeue()} objects. Like a
 * {@link ByteDiskQueue}, and instance of this class has a {@link #suspend()} method that releases
 * the file handle without deleting the dump file. The file handle is reinitialized lazily (which
 * implies that a certain number of {@link #enqueue(Object)}/{@link #dequeue()} calls can be performed on a suspended
 * class without actually reopening the dump file).
 *
 * <p>Similarly to a {@link ByteDiskQueue}, you can {@link #freeze()} and then
 * {@linkplain #createFromFile(long, File, int, boolean) reopen} a {@link ObjectDiskQueue}.
 *
 * @see ByteDiskQueue
 */

public class ObjectDiskQueue<T> implements Closeable, Size64 {

	/** The underlying byte disk queue. */
	private final ByteDiskQueue byteDiskQueue;
	/** The buffer used to serialize/deserialize objects. */
	private final FastByteArrayOutputStream fbaos;
	/** The number of objects in this queue. */
	private long size;

	protected ObjectDiskQueue(ByteDiskQueue byteDiskQueue) {
		this.byteDiskQueue = byteDiskQueue;
		this.fbaos = new FastByteArrayOutputStream(1024);
	}

	/** Creates a new disk-based queue of objects.
	 *
	 * @param file the file that will be used to dump the queue on disk.
	 * @param bufferSize the number of items in the circular buffer (will be possibly decreased so to be a power of two).
	 * @param direct whether the {@link ByteBuffer} used by this queue should be {@linkplain ByteBuffer#allocateDirect(int) allocated directly}.
	 * @see ByteDiskQueue#createNew(File, int, boolean)
	 */
	public static <T> ObjectDiskQueue<T> createNew(final File file, final int bufferSize, final boolean direct) throws IOException {
		return new ObjectDiskQueue<>(ByteDiskQueue.createNew(file,  bufferSize, direct));
	}

	/** Creates a new disk-based queue of objects using an existing file.
	 *
	 * <p>Note that you have to supply the correct number of objects contained in the dump file of
	 * the underlying {@link ByteDiskQueue}. Failure to do so will cause unpredictable behaviour.
	 *
	 * @param size the number of objects contained in {@code file}.
	 * @param file the file that will be used to dump the queue on disk.
	 * @param bufferSize the number of items in the circular buffer (will be possibly decreased so to be a power of two).
	 * @param direct whether the {@link ByteBuffer} used by this queue should be {@linkplain ByteBuffer#allocateDirect(int) allocated directly}.
	 * @see ByteDiskQueue#createFromFile(File, int, boolean)
	 */
	public static <T> ObjectDiskQueue<T> createFromFile(final long size, final File file, final int bufferSize, final boolean direct) throws IOException {
		final ObjectDiskQueue<T> byteArrayDiskQueue = new ObjectDiskQueue<>(ByteDiskQueue.createFromFile(file, bufferSize, direct));
		byteArrayDiskQueue.size = size;
		return byteArrayDiskQueue;
	}

	/** Enqueues an object to this queue.
	 *
	 * @param o the object to be enqueued.
	 */
	public synchronized void enqueue(final T o) throws IOException {
		assert o != null;
		fbaos.reset();
		BinIO.storeObject(o, fbaos);
		byteDiskQueue.enqueueInt(fbaos.length);
		byteDiskQueue.enqueue(fbaos.array, 0, fbaos.length);
		size++;
	}

	/** Dequeues an object from the queue in FIFO fashion. */
	@SuppressWarnings("unchecked")
	public synchronized T dequeue() throws IOException {
		final int length = byteDiskQueue.dequeueInt();
		fbaos.array = ByteArrays.grow(fbaos.array, length);
		byteDiskQueue.dequeue(fbaos.array, 0, length);
		size--;
		try {
			return (T)BinIO.loadObject(new FastByteArrayInputStream(fbaos.array, 0, length));
		}
		catch (final ClassNotFoundException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
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
