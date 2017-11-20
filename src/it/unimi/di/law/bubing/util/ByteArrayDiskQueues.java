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

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.longs.LongHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.NoSuchElementException;

//RELEASE-STATUS: DIST

/** A set of memory-mapped queues of byte arrays.
 *
 * <p>An instance of this class handles a database of FIFO queues. Each queue is associated with a key (which is looked up by {@linkplain Reference2ObjectMap reference},
 * for efficiency). Users can {@linkplain #enqueue(Object, byte[], int, int) enqueue}
 * and {@linkplain #dequeue(Object) dequeue} elements associated with a key in FIFO order. It is also possible
 * to {@linkplain #remove(Object) remove all} elements associated with a key.
 *
 * <p>The {@linkplain #ratio() ratio} between the used and allocated space can be checked periodically,
 * and the method {@link #collect(double)} can be used to compact elements until a target ratio
 * is reached.
 *
 * <p>Note that the metadata associated with all queues must fit into memory. The caching of the
 * content of the log files is performed at the operating system level by the memory-mapping system,
 * however, and does not use the Java heap.
 *
 * <h2>Internals</h2>
 *
 * <p>Queues are stored using a set of memory-mapped append-only log files. When garbage collection frees completely
 * a file, it is deleted. Each element contains a pointer to the position of the next element.
 *
 * <p>{@linkplain #collect(double) Garbage collection} is performed without keeping track of the
 * free space beforehand. Keys are kept in a queue prioritized by the pointer to the last element
 * of the associated FIFO queue that has been read (initially, the first element).
 * As we advance each pointer, we discover free space and we compact the structure.
 */

public class ByteArrayDiskQueues implements Closeable, Size64 {
	private static final boolean DEBUG = false;
	/** By default, we use 64 MiB log files. */
	public static final int DEFAULT_LOG2_LOG_FILE_SIZE = 26;

	/** Metadata associated with a queue. */
	public static final class QueueData implements Serializable {
		private static final long serialVersionUID = 1L;
		/** The pointer to the head of the list (the least recently enqueued, but not dequeued, element). */
		public long head;
		/** The pointer to the tail of the list  (the most recently enqueued element). */
		public long tail;
		/** The number of elements in the list (always nonzero). */
		public long count;
		/** The number of bytes used by the list. */
		public long usage;
	}

	/** The base 2 logarithm of the byte size of a log file. */
	protected final int log2LogFileSize;
	/** The byte size of a log file. */
	protected final int logFileSize;
	/** The mask to extract the position inside a log file from a pointer. A pointer is formed by a position in the lowest
	 * {@link #log2LogFileSize} bits and a log-file index in the remainig upper bits. */
	protected final int logFilePositionMask;
	/** For each key, the associated {@link QueueData}. If a key is present, there is at least one associated element in the queue. */
	public final Reference2ObjectOpenHashMap<Object,QueueData> key2QueueData;
	/** For each log-file index, the associated {@link RandomAccessFile}. An entry might be {@code null} if the log file has been deleted or it has not been opened yet. */
	public final ObjectArrayList<RandomAccessFile> files;
	/** For each log-file index, the associated {@link ByteBuffer}. An entry might be {@code null} if the log file has been deleted or it has not been opened yet. */
	public final ObjectArrayList<ByteBuffer> buffers;
	/** The overall number of elements in the queues. */
	public long size;
	/** The overall number of bytes used by elements in the queues. */
	public long used;
	/** The overall number of bytes allocated (a multiple of {@link #logFileSize}). */
	public long allocated;
	/** The current pointer at which new elements can be appended. */
	public long appendPointer;
	/** The index of the {@linkplain #currBuffer current buffer}. */
	private int currBufferIndex;
	/** The current buffer. */
	private ByteBuffer currBuffer;
	/** The directory there the log files must be created. */
	private File dir;

	/** Creates a set of byte-array disk queues in the given directory using
	 * log files of size 2<sup>{@value #DEFAULT_LOG2_LOG_FILE_SIZE}</sup>.
	 *
	 * @param dir a directory.
	 */
	public ByteArrayDiskQueues(final File dir) {
		this(dir, DEFAULT_LOG2_LOG_FILE_SIZE);
	}

	/** Creates a set of byte-array disk queues in the given directory using the specified
	 * file size.
	 *
	 * @param dir a directory.
	 * @param log2LogFileSize the base-2 logarithm of the size of a log file.
	 */
	public ByteArrayDiskQueues(final File dir, final int log2LogFileSize) {
		this.dir = dir;
		this.log2LogFileSize =log2LogFileSize;
		logFileSize = 1 << log2LogFileSize;
		logFilePositionMask = (1 << log2LogFileSize) - 1;
		key2QueueData = new Reference2ObjectOpenHashMap<>();
		files = new ObjectArrayList<>();
		buffers = new ObjectArrayList<>();
	}

	/** Returns the name of a log file, given its index.
	 *
	 * @param logFileIndex the index of a log file.
	 * @return its name ({@code logFileIndex} in hexadecimal zero-padded to eight digits).
	 */
	private File file(final int logFileIndex) {
		final String t = "00000000" + Integer.toHexString(logFileIndex);
		return new File(dir, t.substring(t.length() - 8));
	}

	/** Returns the index of the buffer associated with a pointer.
	 *
	 * @param pointer a pointer.
	 * @return  the index of the buffer associated with {@code pointer}.
	 */
	private int bufferIndex(final long pointer) {
		return (int)(pointer >>> log2LogFileSize);
	}

	/** Returns the buffer position associated with a pointer.
	 *
	 * @param pointer a pointer.
	 * @return the buffer position associated with {@code pointer}.
	 */
	private int bufferPosition(final long pointer) {
		return (int)(pointer & logFilePositionMask);
	}

	/** Enqueues an element (specified as a byte array) associated with a given key.
	 *
	 * <p>The element is a sequence of bytes specified as an array fragment.
	 *
	 * @param key a key.
	 * @param array a byte array.
	 */
	public void enqueue(final Object key, byte[] array) throws FileNotFoundException, IOException {
		enqueue(key, array, 0, array.length);
	}

	/** Enqueues an element (specified as a byte-array fragment) associated with a given key.
	 *
	 * @param key a key.
	 * @param array a byte array.
	 * @param offset the first valid byte in {@code array}.
	 * @param length the number of valid elements in {@code array}.
	 */
	public void enqueue(final Object key, byte[] array, final int offset, final int length) throws FileNotFoundException, IOException {
		QueueData queueData = key2QueueData.get(key);
		if (queueData == null) {
			queueData = new QueueData();
			queueData.head = appendPointer;
			synchronized (key2QueueData) {
				key2QueueData.put(key, queueData);
			}
		}
		else {
			pointer(queueData.tail);
			writeLong(appendPointer);
		}

		queueData.count++;
		queueData.tail = appendPointer;

		final long start = appendPointer;
		pointer(appendPointer);
		writeLong(0);
		encodeInt(length);
		write(array, offset, length);
		appendPointer = pointer();
		final long bytes = appendPointer - start;
		used += bytes;
		queueData.usage += bytes;

		assert used >= 0 : used;
		size++;
	}

	/** Dequeues the first element available for a given key.
	 *
	 * @param key a key.
	 * @return the first element associated with {@code key}.
	 */
	public byte[] dequeue(final Object key) throws IOException {
		final QueueData queueData = key2QueueData.get(key);
		if (queueData == null) throw new NoSuchElementException();

		final long head = queueData.head;
		pointer(queueData.head);
		queueData.count--;
		queueData.head = readLong();
		final int length = decodeInt();
		final byte[] result = new byte[length];
		read(result, 0, length);
		final long bytes = pointer() - head;
		used -= bytes;
		queueData.usage -= bytes; // If we are dequeuing the last element, this is done on a throw-away QueueData
		if (queueData.count == 0) remove(key);

		size--;
		assert used >= 0 : used;
		return result;
	}

	/** Remove all elements associated with a given key.
	 *
	 * <p>Note that this is a constant-time operation that simply deletes the metadata
	 * associated with the specified key.
	 *
	 * @param key a key.
	 */
	public void remove(final Object key) {
		final QueueData queueData;
		synchronized(key2QueueData) {
			queueData = key2QueueData.remove(key);
		}
		if (queueData == null) return;
		size -= queueData.count;
		used -= queueData.usage;
	}

	/** Returns the number of elements associated with the given key.
	 *
	 * <p>This method can be called by multiple threads.
	 *
	 * @param key a key.
	 * @return the number of elements currently associated with {@code key}.
	 */
	public long count(final Object key) {
		final QueueData queueData;
		synchronized(key2QueueData) {
			 queueData = key2QueueData.get(key);
		}
		return queueData == null ? 0 : queueData.count;
	}

	/** Returns the number of keys.
	 *
	 * @return the number of keys.
	 */
	public int numKeys() {
		 return key2QueueData.size();
	}

	/** Reads a byte at the current pointer.
	 *
	 * @return the byte at the current pointer.
	 */
	protected int read() throws IOException {
		if (! currBuffer.hasRemaining()) nextBuffer(); // Note that this can create a new log file.
		return currBuffer.get() & 0xFF;
	}

	/** Reads a long at the current pointer.
	 *
	 * @return the long at the current pointer.
	 */
	protected long readLong() throws IOException {
		long l = 0;
		for(int i = 0; i < 8; i++) {
			l <<= 8;
			l |= read();
		}
		return l;
	}

	/** Reads a specified number of bytes at the current pointer.
	 *
     * @param b the buffer into which the data will be read.
     * @param offset the start offset in array <code>b</code> at which the data will be written.
     * @param length the number of bytes to read.
	 */
	protected void read(final byte[] b, final int offset, final int length) throws IOException {
		if (length == 0) return;
		int read = 0;
		while(read < length) {
			int remaining = currBuffer.remaining();
			if (remaining == 0) {
				nextBuffer();
				remaining = logFileSize;
			}
			currBuffer.get(b, offset + read, Math.min(length - read, remaining));
			read += Math.min(length - read, remaining);
		}
	}

	/** Writes a byte at the current pointer.
	 *
	 * @param b the byte to be written.
	 */
	protected void write(final byte b) throws IOException {
		if (! currBuffer.hasRemaining()) nextBuffer();
		currBuffer.put(b);
	}

	/** Writes a long at the current pointer.
	 *
	 * @param l the long to be written.
	 */
	protected void writeLong(final long l) throws IOException {
		for(int i = 8; i-- != 0;) write((byte)(l >>> (i * 8)));
	}

	/** Writes a specified number of bytes at the current pointer.
	 *
   	 * @param b the data.
   	 * @param offset the start offset in {@code b}.
   	 * @param length the number of bytes to write.
   	 */
	protected void write(final byte[] b, final int offset, final int length) throws IOException {
		if (length == 0) return;
		int written = 0;
		while(written < length) {
			int remaining = currBuffer.remaining();
			if (remaining == 0) {
				nextBuffer();
				remaining = logFileSize;
			}
			currBuffer.put(b, offset + written, Math.min(length - written, remaining));
			written += Math.min(length - written, remaining);
		}
	}

	/** Returns the current pointer.
	 *
	 * @return  the current pointer.
	 */
	public long pointer() {
		return ((long)currBufferIndex << log2LogFileSize) + currBuffer.position();
	}

	/** Sets the current pointer. The associated log file is opened if necessary. */
	public void pointer(final long pointer) throws FileNotFoundException, IOException {
		currBufferIndex = bufferIndex(pointer);
		assert currBufferIndex <= buffers.size();
		if (currBufferIndex == buffers.size() || (currBuffer = buffers.get(currBufferIndex)) == null) {
			if (currBufferIndex == buffers.size()) {
				files.size(currBufferIndex + 1);
				buffers.size(currBufferIndex + 1);
			}
			// We open the buffer associated with currBufferIndex.
			final File file = file(currBufferIndex);
			if (! file.exists()) allocated += logFileSize;
			final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
			files.set(currBufferIndex, randomAccessFile);
			buffers.set(currBufferIndex, currBuffer = randomAccessFile.getChannel().map(MapMode.READ_WRITE, 0, logFileSize));
		}
		currBuffer.position(bufferPosition(pointer));
	}

	/** Creates a new buffer, or opens an old one, at the current pointer, which must be a multiple of {@link #logFileSize}. */
	private void nextBuffer() throws FileNotFoundException, IOException {
		assert (pointer() & logFilePositionMask) == 0;
		pointer(pointer());
	}

	public double ratio() {
		if (size == 0) return 1;
		if (DEBUG) System.err.println("Returning ratio " + (double)used / (allocated - ((logFileSize - bufferPosition(appendPointer)) & logFilePositionMask)) + "; used=" + used + ", allocated=" + allocated);
		return (double)used / (allocated - ((logFileSize - bufferPosition(appendPointer)) & logFilePositionMask));
	}

	/** Computes the ratio between {@linkplain #used} space and {@linkplain #allocated}
	 * space minus the gain plus one times {@link #logFileSize}.
	 *
	 * @param gain the number of buffers gained.
	 * @return the ratio between {@linkplain #used used} space and {@linkplain #allocated}
	 * space minus {@code gain} plus one times {@link #logFileSize}.
	 */
	private double gainedRatio(long gain) {
		if (size == 0) return 1;
		return (double)used / (allocated - ((logFileSize - bufferPosition(appendPointer)) & logFilePositionMask) - (gain << log2LogFileSize));
	}

	/** Performs garbage collection until {@link #ratio()} is greater than the specified target ratio.
	 *
	 * @param targetRatio a {@link #ratio()} to reach.
	 */
	public void collect(final double targetRatio) throws IOException {
		final int n = key2QueueData.size();
		if (DEBUG) System.err.println("Collection required, ratio=" + ratio());
		if (n == 0 || ratio() >= targetRatio) return;

		if (DEBUG) System.err.println("Starting collection: used=" + used + ", allocated=" + allocated + ", ratio=" + ratio() + ", target ratio=" + targetRatio);
		final Object[] key = new Object[n];
		final long[] currentPointer = new long[n];
		final long[] previousPointer = new long[n];
		final int[] array = new int[n];
		// Dump the metadata map into an array of keys and a parallel key of head pointers.
		ObjectIterator<Reference2ObjectMap.Entry<Object, QueueData>> fastIterator = key2QueueData.reference2ObjectEntrySet().fastIterator();
		for(int i = n; i-- != 0;) {
			Reference2ObjectMap.Entry<Object, QueueData> e = fastIterator.next();
			key[i] = e.getKey();
			previousPointer[i] = currentPointer[i] = e.getValue().head;
			array[i] = i;
		}

		// This queue holds the current pointer for each FIFO queue.
		final LongHeapSemiIndirectPriorityQueue queue = new LongHeapSemiIndirectPriorityQueue(currentPointer, array);
		// Remembers which log file will be deleted at the end.
		final LongArrayBitVector deleted = LongArrayBitVector.ofLength(buffers.size());

		long collectPointer = currentPointer[queue.first()] & ~logFilePositionMask;
		// Safely remove previous files
		for(int buffer = bufferIndex(collectPointer); buffer-- != 0;) deleteBuffer(buffer);
		long gain = 0; // Keeps track of the number of ones in the deleted bit vector.
		long moved = 0; // Stats
		int top = queue.first();
		byte[] t = new byte[1024];

		// This is just to avoid calling gainedRatio() too much, as it's really slow.
		while((moved & 0x3FF) != 0 || gainedRatio(gain) < targetRatio) {
			// Read element
			pointer(currentPointer[top]);
			moved++;
			final long nextPointer = readLong();
			final int length = decodeInt();
			if (length > t.length) t = new byte[length];
			read(t, 0, length);
			// for(int p = result.length; p-- != 0;) assert result[p] == (byte)p; // Just for unit tests

			assert collectPointer <= currentPointer[top] : Long.toHexString(collectPointer) + " > " + Long.toHexString(currentPointer[top]);

			// Write element at collection point
			final long movedEntryPointer = collectPointer;
			pointer(collectPointer);
			writeLong(nextPointer);
			encodeInt(length);
			write(t, 0, length);
			collectPointer = pointer();

			// Fix pointers
			if (currentPointer[top] == previousPointer[top]) key2QueueData.get(key[top]).head = movedEntryPointer;
			else {
				pointer(previousPointer[top]);
				writeLong(movedEntryPointer);
			}

			previousPointer[top] = movedEntryPointer;
			final long previousCurrentPointerTop = currentPointer[top];
			if (nextPointer != 0) {
				currentPointer[top] = nextPointer;
				assert nextPointer >= collectPointer;
				queue.changed();
			}
			else {
				key2QueueData.get(key[top]).tail = movedEntryPointer;
				queue.dequeue();
				if (queue.isEmpty()) break;
			}


			top = queue.first();

			// Update the information about buffers that will be deleted
			for(int i = bufferIndex(previousCurrentPointerTop); i < bufferIndex(currentPointer[top]); i++)
				if (file(i).exists() && ! deleted.set(i, true)) gain++;

			for(int i = bufferIndex(movedEntryPointer);  i <= bufferIndex(collectPointer); i++)
				if (deleted.set(i, false)) gain--;

			assert gain == deleted.count() : gain + " != " + deleted.count() + " " + deleted;
		}

		if (queue.isEmpty()) {
			// We moved all elements. Move append pointer to the end of the collected elements and delete all following buffers.
			appendPointer = collectPointer;
			final int usedBuffers = bufferIndex(collectPointer) + 1;
			for(int buffer = usedBuffers; buffer < buffers.size(); buffer++) deleteBuffer(buffer);
			buffers.size(usedBuffers);
			files.size(usedBuffers);
			assert ratio() == 1 : ratio() + " != 1";
		}
		else {
			// Delete buffers marked as such.
			int d = 0;
			for(int buffer = bufferIndex(collectPointer + logFileSize - 1); buffer < bufferIndex(currentPointer[top]); buffer++)
				if (deleteBuffer(buffer)) d++;
			assert d == gain : deleted + " != " + gain;
			assert ratio() >= targetRatio : ratio() + " < " + targetRatio;
		}

		if (DEBUG) System.err.println("Ending collection: used=" + used + ", allocated=" + allocated + ", ratio=" + ratio() + ", moved " + moved + " elements (" + 100.0 * moved / size64() + "%)");
	}

	/** Deletes a buffer, it it exists, updating {@link #buffers} and {@link #files}.
	 *
	 * <p>Note that existence is checked in the {@link #buffers} array, not on the filesystem.
	 *
	 * @param buffer a buffer index.
	 * @return true if the buffer of given index exists.
	 */
	private boolean deleteBuffer(final int buffer) throws IOException {
		if (buffers.get(buffer) != null) {
			buffers.set(buffer, null);
			files.get(buffer).close();
			files.set(buffer,  null);
			file(buffer).delete();
			allocated -= logFileSize;
			return true;
		}
		else {
			assert ! file(buffer).exists();
			return false;
		}
	}


	/** Encodes using vByte a nonnegative integer at the current pointer.
	 * @param value a nonnegative integer.
	 */
	protected int encodeInt(final int value) throws IOException {
		if (value < (1 << 7)) {
			write((byte)value);
			return 1;
		}

		if (value < (1 << 14)) {
			write((byte)(value >>> 7 | 0x80));
			write((byte)(value & 0x7F));
			return 2;
		}

		if (value < (1 << 21)) {
			write((byte)(value >>> 14 | 0x80));
			write((byte)(value >>> 7 | 0x80));
			write((byte)(value & 0x7F));
			return 3;
		}


		if (value < (1 << 28)) {
			write((byte)(value >>> 21 | 0x80));
			write((byte)(value >>> 14 | 0x80));
			write((byte)(value >>> 7 | 0x80));
			write((byte)(value & 0x7F));
			return 4;
		}

		write((byte)(value >>> 28 | 0x80));
		write((byte)(value >>> 21 | 0x80));
		write((byte)(value >>> 14 | 0x80));
		write((byte)(value >>> 7 | 0x80));
		write((byte)(value & 0x7F));
		return 5;
	}

	/** Decodes using vByte a nonnegative integer at the current pointer.
	 *
	 * @return a nonnegative integer decoded using vByte.
	 */
	protected int decodeInt() throws IOException {
		for(int x = 0; ;) {
			final int b = read();
			x |= b & 0x7F;
			if ((b & 0x80) == 0) return x;
			x <<= 7;
		}
	}

	/** Returns the overall number of elements in the queues.
	 * @return the overall number of elements in the queues.
	 */
	@Override
	public long size64() {
		return size;
	}

	@Override
	@Deprecated
	public int size() {
		return (int)Math.min(Integer.MAX_VALUE,  size);
	}

	/** Closes all files. */
	@Override
	public void close() throws IOException {
		for(RandomAccessFile file: files) if (file != null) file.close();
	}

}
