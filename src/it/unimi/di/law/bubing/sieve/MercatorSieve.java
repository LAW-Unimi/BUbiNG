package it.unimi.di.law.bubing.sieve;

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

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.sux4j.mph.AbstractHashFunction;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** A concrete implementation of an {@link AbstractSieve} that stores hash
 * data on disk, much in the same way as it was suggested by Allan Heydon and Marc Najork in
 * &ldquo;<a href="http://www.cs.tamu.edu/academics/tr/tamu-cs-tr-2008-2-2">Mercator: A Scalable, Extensible Web Crawler</a>&rdquo;,
 * <i>World Wide Web</i>, (2)4:219&minus;229, 1999, Springer.
 *
 * <p>Each key known to the sieve is stored as a 64-bit hash in a disk file. At each {@link #enqueue(Object, Object)} the hash of the
 * enqeueud key is added to a bucket, and the key is saved in an auxiliary file. When the bucket is full it sorted and compared with
 * the set of keys known to the sieve. Note that the output order is guaranteed to be the same of the input order (i.e., keys
 * are {@linkplain AbstractSieve.NewFlowReceiver#append(long, Object) appended} in the same order in which they appeared the first time).
 */
public class MercatorSieve<K,V> extends AbstractSieve<K,V> {
	private static Logger LOGGER = LoggerFactory.getLogger(MercatorSieve.class);

	/** A data structure to keep keys (on an auxiliary disk file) and their 64-bit hash values (in a fixed-size buffer). After filling
	 *  it, one can {@linkplain #prepare() start} to {@linkplain #consumeKey() consume the keys} it contains. */
	private final static class Bucket<K> implements Closeable {
		/** The object used to store keys onto the file. */
		private ByteSerializerDeserializer<K> serializer;
		/** The number of keys currently in the bucket. */
		private int items;
		/** The size of the bucket (maximum number of keys it can hold). */
		private final int size;
		/** The hash values of the keys currently in the bucket (its only meaningful elements are those of indices less than {@link #items}). */
		private final long[] buffer;
		/** The auxiliary file used to store the keys. */
		private final File auxFile;
		/** The input stream associated with the auxiliary file. */
		private FastBufferedInputStream auxFbis;
		/** The output stream associated with the auxiliary file. */
		private final FastBufferedOutputStream aux;
		/** The buffer used for the output stream. */
		private byte[] ioBuffer;

		/** Creates a bucket.
		 *
		 * @param bucketSize the size (in items) of the bucket.
		 * @param bufferSize the size (in bytes) of the buffer to be used for the output stream.
		 * @param sieveDir the directory where the auxiliary file should be opened.
		 * @param serializer the serializer to be used for storing the keys.
		 * @throws IOException
		 */
		public Bucket(final int bucketSize, final int bufferSize, final File sieveDir, final ByteSerializerDeserializer<K> serializer) throws IOException {
			this.serializer = serializer;
			this.ioBuffer = new byte[bufferSize];
			// buffer
			items = 0;
			size = bucketSize;
			buffer = new long[bucketSize];
			// aux
			auxFile = new File(sieveDir, "aux");
			aux = new FastBufferedOutputStream(new FileOutputStream(auxFile), ioBuffer);
		}

		/** Adds a new key.
		 *
		 * @param hash the 64-bit hash of the key.
		 * @param key the key.
		 * @throws IOException
		 */
		// ALERT: we ignore values
		public void append(final long hash, final K key) throws IOException {
			buffer[items++] = hash;
			serializer.toStream(key, aux);
		}

		/** Checks if the bucket is full.
		 *
		 * @return {@code true} iff the bucket is full.
		 */
		public boolean isFull() {
			return items == size;
		}

		/** Prepares the bucket to be consumed.
		 *
		 * @throws IOException
		 */
		public void prepare() throws IOException {
			aux.flush();
			auxFbis = new FastBufferedInputStream(new FileInputStream(auxFile), ioBuffer);
		}

		/** Returns the next key to be consumed.
		 *
		 * @return the next key.
		 * @throws IOException
		 */
		public K consumeKey() throws IOException {
			if (auxFbis == null) throw new IllegalStateException();
			return serializer.fromStream(auxFbis);
		}

		/** Skips one key.
		 *
		 * @throws IOException
		 */
		public void skipKey() throws IOException {
			if (auxFbis == null) throw new IllegalStateException();
			serializer.skip(auxFbis);
		}

		/** Clears the bucket (making it ready to be re-used for filling). */
		public void clear() throws IOException {
			items = 0;
			auxFbis.close();
			auxFbis = null;
			aux.position(0);
		}

		@Override
		public void close() {
			auxFile.delete();
		}
	}

	// TODO: find another name.
	// TODO: comment this!
	private static class Store {
		private final File name;
		private final File outputFile;
		private final ByteBuffer outputBuffer;
		private final ByteBuffer inputBuffer;
		private FileChannel inputChannel;
		private FileChannel outputChannel;

		public Store(boolean sieveIsNew, final File sieveDir, final String name, final int bufferSize) throws IOException {
			this.name = new File(sieveDir, name);
			if (sieveIsNew && ! this.name.createNewFile()) throw new IOException("Sieve store " + this.name + " exists");
			if (! sieveIsNew && ! this.name.exists())  throw new IOException("Can't find sieve store " + this.name);
			this.inputBuffer = ByteBuffer.allocateDirect(bufferSize & -1 << 3).order(ByteOrder.nativeOrder());
			this.outputBuffer = ByteBuffer.allocateDirect(bufferSize & -1 << 3).order(ByteOrder.nativeOrder());
			this.outputFile = new File(sieveDir, name + "~");
		}

		@SuppressWarnings("resource")
		public long open() throws IOException {
			outputChannel = new FileOutputStream(outputFile).getChannel();
			inputChannel = new FileInputStream(name).getChannel();
			outputBuffer.clear();
			inputBuffer.clear();
			inputBuffer.flip();
			return name.length() / (Long.SIZE / Byte.SIZE);
		}

		public void append(final long v) throws IOException {
			outputBuffer.putLong(v);
			if (! outputBuffer.hasRemaining()) {
				outputBuffer.flip();
				outputChannel.write(outputBuffer);
				outputBuffer.clear();
			}
		}

		public long consume() throws IOException {
			if (! inputBuffer.hasRemaining()) {
				inputBuffer.clear();
				inputChannel.read(inputBuffer);
				inputBuffer.flip();
			}
			return inputBuffer.getLong();
		}

		public void close() throws IOException {
			outputBuffer.flip();
			outputChannel.write(outputBuffer);
			outputChannel.close();
			inputChannel.close();
			if (! name.delete()) throw new IOException("Cannot delete store file " + name);
			if (! outputFile.renameTo(name)) throw new IOException("Cannot rename new store file " + outputFile + " to " + name);
		}
	}

	private final Store store;
	private final Bucket<K> bucket;
	private volatile boolean closed;
	private final int[] position;

	/** Creates a new Mercator-like sieve.
	 *
	 * @param sieveIsNew whether we are creating a new sieve or opening an old one.
	 * @param sieveDir a directory for storing the sieve files.
	 * @param sieveSize the size of the size in longs.
	 * @param storeIOBufferSize the size in bytes of the buffer used to read and write the hash store during flushes (allocated twice during flushes).
	 * @param auxFileIOBufferSize the size in bytes of the buffer used to read and write the auxiliary file (always allocated; another allocation happens during flushes).
	 * @param newFlowReceiver a receiver for the flow of new keys.
	 * @param keySerDeser a serializer/deserializer for keys.
	 * @param valueSerDeser a serializer/deserializer for values.
	 * @param hashingStrategy a hashing strategy for keys.
	 * @param updateStrategy the strategy used to update the values associated to duplicate keys.
	 */
	public MercatorSieve(final boolean sieveIsNew, final File sieveDir, final int sieveSize, final int storeIOBufferSize, final int auxFileIOBufferSize, final NewFlowReceiver<K> newFlowReceiver, final ByteSerializerDeserializer<K> keySerDeser, final ByteSerializerDeserializer<V> valueSerDeser,
			final AbstractHashFunction<K> hashingStrategy, final UpdateStrategy<K, V> updateStrategy)
			throws IOException {
		super(keySerDeser, valueSerDeser, hashingStrategy, updateStrategy);

		LOGGER.info("Creating Mercator sieve of size " + sieveSize + " (" + Util.formatSize2(sieveSize * 12L) + " bytes), store I/O buffer size " + storeIOBufferSize + " and aux-file I/O buffer size " + auxFileIOBufferSize);

		setNewFlowRecevier(newFlowReceiver);

		if ((storeIOBufferSize & 0x7) != 0) throw new IllegalArgumentException("Store I/O buffer size length must be a multiple of 8");

		bucket = new Bucket<>(sieveSize, auxFileIOBufferSize, sieveDir, keySerDeser);
		store = new Store(sieveIsNew, sieveDir, "store", storeIOBufferSize);
		position = new int[sieveSize];
	}

	@Override
	public void close() throws IOException {
		closed = true;
		flush();
		bucket.close();
	}

	@Override
	public boolean enqueue(K key, V value) throws IOException, InterruptedException {
		if (closed) throw new IllegalStateException();
		final long hash = hashingStrategy.getLong(key);
		synchronized(this) {
			bucket.append(hash, key);
			if (bucket.isFull()) {
				flush();
				return true;
			}
			else return false;
		}
	}

	public int numberOfItems(){
		return bucket.items;
	}


	@Override

	public synchronized void flush() throws IOException {
		final long start = System.nanoTime();
 		LOGGER.info("Flush started.");

 		// TODO: why doesn't it work even when just flushed?
		if (bucket.items == 0) {
			if (LOGGER.isDebugEnabled()) LOGGER.debug("Nothing to be flushed: returning...");
			return; // Just flushed!
		}

		final long storeSize = store.open();
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Store size: " + storeSize);
		newFlowReceiver.prepareToAppend();

		long next = -1;
		long count = 0;
		// The position in the store file of next, or storeSize if next is not valid.
		long storePosition = 0;
		long newHashes = 0;
		if (storeSize != 0) next = store.consume();

		final int numberOfItems = bucket.items;
		bucket.prepare();

		final int[] position = this.position;
		final long[] buffer = bucket.buffer;

		for(int i = numberOfItems; i-- != 0;) position[i] = i;

		LongArrays.parallelRadixSortIndirect(position, buffer, 0, numberOfItems, false);
		LongArrays.stabilize(position, buffer, 0, numberOfItems);

		int dups = 0;

		final long endBucketSorted = System.nanoTime();
		LOGGER.info("Bucket sorted (" + numberOfItems + " items)");

		for(int j = 0; j < numberOfItems; j++) {
			final long hash = buffer[position[j]];
			final int k = j;
			// We invalidate duplicates but keep the entry that was enqueued earlier.

			while(j < numberOfItems - 1 && buffer[position[j + 1]] == hash) {
				position[++j] = Integer.MAX_VALUE;
				dups++;
			}

			for(;;) {
				if (storePosition == storeSize || hash < next) {
					// The bucket key is new. ALERT: no value!
					//System.err.println("Flush #" + debugFlushCount + ": key with hash " + hash + " is new");
					store.append(hash);
					newHashes++;
					break;
				}
				else if (next == hash) {
					// Existing key: copy to new store with updated value. ALERT: no value!
					store.append(next);
					position[k] = Integer.MAX_VALUE;
					if (storePosition < storeSize - 1) next = store.consume();
					storePosition++;
					break;
				}
				else if (next < hash){
					// Old key, just copy to new store. ALERT: no value!
					store.append(next);
					if (storePosition < storeSize - 1) next = store.consume();
					storePosition++;
				}
			}
		}

		final long endFusion = System.nanoTime();
		LOGGER.info("Fusion with existing store completed (" + Util.format(storePosition + newHashes) + " hashes, " + Util.format(1E9 * (storePosition + newHashes) / Math.max(endFusion - endBucketSorted , 1)) + " hashes/s)");

		IntArrays.parallelQuickSort(position, 0, numberOfItems);

		final long endPositionSorted = System.nanoTime();
		LOGGER.info("Positions sorted");

		int auxInPosition = 0;
		for(int j = 0; j < numberOfItems && position[j] != Integer.MAX_VALUE; j++) {
			while(auxInPosition < position[j]) {
				bucket.skipKey();
				auxInPosition++;
			}
			newFlowReceiver.append(buffer[position[j]], bucket.consumeKey());
			count++;
			auxInPosition++;
		}

		newFlowReceiver.finishedAppending();

		long endFlowReceiverAppending = System.nanoTime();

		bucket.clear();

		LOGGER.info("Fill: " + 100.0 * numberOfItems / bucket.size + " %");
		LOGGER.info("Unique keys: " + Util.format(100 - 100.0 * dups / numberOfItems) + " %");

		// This part is out of the right timing zone (it's part of the fusion process), but it's so small that it's OK.
		while (storePosition < storeSize) {
			store.append(next);
			if (storePosition < storeSize - 1) next = store.consume();
			storePosition++;
		}

		store.close();

		long end = System.nanoTime();
		double duration = Math.max(end - start, 1);
		LOGGER.info("Flush completed (" + count + " keys appended, " + Util.format((end - start) / 1E9) + "s)");
		LOGGER.info("BucketSorting: " + Util.format(100.0 * Math.max(endBucketSorted - start , 0) / duration) + "%" +
				" Fusion: " + Util.format(100.0 * Math.max(endFusion - endBucketSorted , 0) / duration) + "%" +
				" PositionSorting: " + Util.format(100.0 * Math.max(endPositionSorted - endFusion , 0) / duration) + "%" +
				" FlowReceiverAppending: " + Util.format(100.0 * Math.max(endFlowReceiverAppending - endPositionSorted , 0) / duration) + "%");

	}
}
