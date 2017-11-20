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

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.bits.TransformationStrategy;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.AbstractHashFunction;
import it.unimi.dsi.sux4j.mph.Hashes;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

//RELEASE-STATUS: DIST

/**
 * A sort of a map, that handles (key,value) pairs of generic type. You can think of it as a long-term sieve where you can put the same k as
 * many times as you want, but that provides the following guarantees: <ul> <li>every key that is
 * {@linkplain AbstractSieve#enqueue(Object, Object) enqueued} will eventually be
 * dequeued; <li>only enqueued keys will be dequeued; <li>every key
 * will be dequeued just once. </ul>
 * Broadly, you can think of all the sieve logic as if it was like a <code>uniq</code> filter (in the sense of UN*X).
 * Here we are completely disregarding the asynchronous logic of dequeuing, which of course impacts the behaviour.
 *
 * <p>This data structure generalizes and adapts that described in the paper
 * <a href="http://www.cs.tamu.edu/academics/tr/tamu-cs-tr-2008-2-2"><em>IRLbot: Scaling to 6 Billion Pages and Beyond</em></a>
 * by Lee et al., where it was called <em>DRUM</em> (Disk Repository with Update Management). We prefer not to refer anymore to <em>disk</em>
 * in our abstract version because different implementations may decide not to use the disk in a strict sense to hold the items.
 *
 *  <p>The structure was originally described as follows:
 *  <ul>
 *  	<li>it is basically a way to store (k,v) pairs on an external data structure (we prefer to call them k and v instead of keys and values because we shall use
 *  the word key with a different meaning); k's present in the store are unique (that is, no two pairs in the store can have the same k);
 *  	<li>the data structure offers three kinds of operations, called <em>check</em>, <em>update</em> and <em>check+update</em>; typically, such operations are batched (that is,
 *  they are performed on a large input), but for the moment we prefer to describe them as if they were performed on a single input:
 *  		<ul>
 *  			<li><em>check(k,v,x)</em> takes a triple (k,v,x) (x is called the auxiliary data), and determines if k is new (i.e., no pair with k is present in the store already)
 *  or not; if it is new, the check operation just returns the triple itself (k,v,x); otherwise, it returns (k,v,x,w) where w is the value that is associated with k in the store;
 *  			<li><em>update(k,v,x)</em> if k is new, adds the pair (k,v) to the store; otherwise, i.e. if some pair (k,w) is present in the store, it changes it to (k,up(k,v,x,w)) where
 *  up is some suitable update function;
 *  			<li><em>check+update(k,v,x)</em> performs a check followed by an update, and returns what the check part would return;
 *  		</ul>
 *  	<li>actually, as we said, all operations are performed in batch and asynchronously; so, for example, <em>check</em> typically takes as input a batch of triples, and divides it into two flows:
 *  one is a copy of the triples containing new k's, the other is made of quadruples for duplicated k's (note that if some k appears more than once in the input batch, only the
 *  first time it may be considered as new). These two flows are asynchronously accessible through some other method (see below).
 *  </ul>
 *
 *
 *  <p>This implementation of the data structure is different from the one described above in the following regards:
 *  <ul>
 *  	<li>we assume (as implicitly done in the original paper) that k is a suitable hash of x, so all functions only take as input (x,v) (where x is called key and v is called a
 *  value) and k is produced as some hash applied to x (the hashing strategy is provided at construction time);
 *  	<li>only the check+update operation is available, with the following additional limitation: the flow of quadruples (i.e., the flow containing duplicate k's) is simply
 *  discarded; the update strategy (the function up described above) is by default defined by up(k,v,x,w)=v (i.e., substitute the old value with the new one), but may be changed
 *  (specifying a different update strategy at construction time): observe that, actually, up doesn't take (k,v,x,w) as input but rather (x,v,w), because of the above described change
 *  in the treatment of keys/auxiliary data;
 *      <li>the call to <em>check+update</em> is actually decoupled into two phases: the method {@link #enqueue(Object, Object)} corresponds to the call: it should be very fast,
 *  although some implementation may require it to be blocking, and will perform the check (and update) sometime in the future; when a {@link AbstractSieve#flush()} is performed,
 *  a new flow of pairs (the pairs that the  check method would return) is created: those pairs are actually provided to a {@link AbstractSieve.NewFlowReceiver} object
 *  that can decide what to do with them (see the {@link AbstractSieve.NewFlowReceiver} documentation).
 *  </ul>
 *
 *  <h2>Thread safety</h2>
 *
 *  <p>Implementors should guarantee that all method calls are thread-safe, and as efficient
 *  as possible. The {@link #enqueue(Object, Object)} method should usually be non-blocking,
 *  but some bounded implementations may decide to make it wait until at least one element is
 *  dequeued: this event should be carefully documented. A call to {@link #flush()} guarantees that all new elements
 *  that have not yet been dequeued are made available, if any.
 */

public abstract class AbstractSieve<K,V> implements Closeable {

	protected final ByteSerializerDeserializer<K> keySerDeser;
	protected final ByteSerializerDeserializer<V> valueSerDeser;
	protected final AbstractHashFunction<K> hashingStrategy;
	protected final UpdateStrategy<K,V> updateStrategy;
	protected NewFlowReceiver<K> newFlowReceiver;

	/** An object that can receive a new flow of hash/key pairs and that
	 *  acts as a listener for the {@link AbstractSieve}. Every time the
	 *  sieve is ready to produce new keys, it will call the {@link #prepareToAppend()}
	 *  method to warn the new flow receiver. Then it will call {@link #append(long, Object)}
	 *  (typically many times in a row) to add the new keys. After that, the
	 *  end of the current new flow of keys is signalled by calling {@link #finishedAppending()}.
	 *  A call to {@link #noMoreAppend()} will, instead, mean that no more keys
	 *  will ever be produced by the sieve that is calling it.
	 */
	public static interface NewFlowReceiver<K> {
		/** A new flow of keys is ready and will start being appended.
		 *
		 * @throws IOException
		 */
		public void prepareToAppend() throws IOException;

		/** A new key is appended.
		 *
		 * @param hash the key hash.
		 * @param key the key itself.
		 * @throws IOException
		 */
		public void append(final long hash, final K key) throws IOException;

		/** The new flow of keys is over.
		 *
		 * @throws IOException
		 */
		public void finishedAppending() throws IOException;

		/** There will be no more new flows (because the sieve that is calling
		 *  this method was closed).
		 *
		 * @throws IOException
		 */
		public void noMoreAppend() throws IOException;
	}

	/** A basic, on-disk {@link AbstractSieve.NewFlowReceiver}. */
	public final static class DiskNewFlow<T> implements NewFlowReceiver<T> {
		private final ByteSerializerDeserializer<T> serializer;
		private final String baseName;
		private long size;
		private long appendSize;

		private DataInputStream input;
		private int inputIndex;

		private DataOutputStream output;
		private int outputIndex;

		private boolean closed;

		public DiskNewFlow(final ByteSerializerDeserializer<T> serializer) throws IOException {
			this.serializer = serializer;
			baseName = File.createTempFile(DiskNewFlow.class.getSimpleName(), "-tmp").toString();
			inputIndex = -1;
			outputIndex = 0;
			size = 0;
		}

		public synchronized long size() {
			return size;
		}

		@Override
		public synchronized void prepareToAppend() throws IOException {
			if (closed) throw new IllegalStateException();
			appendSize = 0;
			output = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(new File(baseName + outputIndex))));
		}

		@Override
		public synchronized void append(final long hash, final T key) throws IOException {
			if (closed) throw new IllegalStateException();
			output.writeLong(hash);
			serializer.toStream(key, output);
			appendSize++;
		}

		@Override
		public synchronized void finishedAppending() throws IOException {
			if (closed) throw new IllegalStateException();
			output.close();
			File f = new File(baseName + outputIndex);
			if (f.length() == 0) f.delete();
			else outputIndex++;
			size += appendSize;
			notifyAll();
		}

		@Override
		public synchronized void noMoreAppend() throws IOException {
			closed = true;
		}

		/** Returns the next key in the flow of new pairs remained after the check, and discards the corresponding value. Note that calling this method is blocking (it may take a long time
		 *  to produce the next pair because check+update is performed in batch); in particular, if no thread calls {@link AbstractSieve#enqueue(Object, Object)} for a sufficient
		 *  number of times or {@link #flush}, a call to this method may never return.
		 *
		 * @return the next key (the value is discarded).
		 * @throws NoSuchElementException if there is no more pair to be returned; this may only happen if this new flow has been {@linkplain #close() closed}.
		 */
		public synchronized MutableString dequeueKey() throws NoSuchElementException, IOException, InterruptedException {
			if (closed && size() == 0) throw new NoSuchElementException();

			while (! closed && size() == 0) {
				wait();
				if (closed && size() == 0) throw new NoSuchElementException();
			}

			assert size() > 0 : size() + " <= 0";

			while(inputIndex == -1 || input.available() == 0) {
				if (inputIndex != -1) {
					input.close();
					new File(baseName + inputIndex).delete();
				}
				File file = new File(baseName + ++inputIndex);
				file.deleteOnExit();
				input = new DataInputStream(new FastBufferedInputStream(new FileInputStream(file)));
			}
			input.readLong(); // ALERT: we throw it away (no values)
			size--;
			return new MutableString().readSelfDelimUTF8((InputStream)input);
		}
	}

	public static final class DefaultUpdateStrategy<K,V> implements UpdateStrategy<K,V> {
		@Override
		public V update(K key, V newValue, V oldValue) {
			return newValue;
		}
	}

	private static final class CharSequenceHashFunction extends AbstractHashFunction<CharSequence> {
		private static final long serialVersionUID = -920229826501456017L;
		private final static TransformationStrategy<CharSequence> transf = TransformationStrategies.iso();

		@Override
		public long getLong(Object key) {
			return Hashes.murmur(transf.toBitVector((CharSequence)key), 0);
		}
	}

	public final static AbstractHashFunction<CharSequence> CHAR_SEQUENCE_HASHING_STRATEGY = new CharSequenceHashFunction();

	/** A (key,value) pair. */
	public static class SieveEntry<K,V> {
		public K key;
		public V value;

		public SieveEntry(final K key, final V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public String toString() {
			return "<" + key + "," + value + ">";
		}
	}

	/** An update strategy: it determines how a stored value should be updated in the presence of duplicate keys.
	 */
	public interface UpdateStrategy<K, V> {
		/** Computes the new value to be put in the store when a duplicate key is found.
		 *
		 * @param key the key that already appears in the store (to be more precise: its hash already appears in the store).
		 * @param newValue the new value associated with the key.
		 * @param oldValue the old value present in the store associated with the key.
		 * @return the new value to store with the key.
		 */
		public V update(final K key, final V newValue, final V oldValue);
	}

	/** Creates a new sieve with the given data.
	 *
	 * @param keySerDeser the serializer and deserializer to be used to store keys.
	 * @param valueSerDeser the serializer and deserializer to be used to store values.
	 * @param hashingStrategy the function to be applied to keys ({@link CharSequence}) to obtain hash values (the store actually contains hash values, not keys).
	 * @param updateStrategy the strategy used to update the values associated to duplicate keys.
	 */
	public AbstractSieve(final ByteSerializerDeserializer<K> keySerDeser, final ByteSerializerDeserializer<V> valueSerDeser, final AbstractHashFunction<K> hashingStrategy, final UpdateStrategy<K,V> updateStrategy) {
		this.keySerDeser = keySerDeser;
		this.valueSerDeser = valueSerDeser;
		this.hashingStrategy = hashingStrategy;
		this.updateStrategy = updateStrategy;
	}

	/** Add the given (key,value) pair to the store.
	 * @return true if the operation caused a flush.
	 */
	public abstract boolean enqueue(final K key, final V value) throws IOException, InterruptedException;

	/** Closes (forever) this sieve. */
	@Override
	public abstract void close() throws IOException;

	/** Sets the receiver for the new flows generated by this sieve.
	 *
	 * @param newFlowReceiver the new flow receiver for this sieve.
	 */
	public void setNewFlowRecevier(final NewFlowReceiver<K> newFlowReceiver) {
		this.newFlowReceiver = newFlowReceiver;
	}

	/** Forces the check+update of all pairs that have been enqueued. */
	public abstract void flush() throws IOException, InterruptedException;
}
