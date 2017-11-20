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

import it.unimi.dsi.sux4j.mph.AbstractHashFunction;

import java.io.IOException;

//RELEASE-STATUS: DIST

/** A sieve that simply (and immediately) copies {@linkplain #enqueue(Object, Object) enqueued keys} to the {@linkplain #setNewFlowRecevier(it.unimi.di.law.bubing.sieve.AbstractSieve.NewFlowReceiver) new flow receiver}.
 *
 * <p>Note that instances of this class call {@link AbstractSieve.NewFlowReceiver#prepareToAppend()} in the constructor only, and
 * {@link AbstractSieve.NewFlowReceiver#noMoreAppend()} in the method {@link #close()} only.
 */

public final class IdentitySieve<K, V> extends AbstractSieve<K, V> {
	public IdentitySieve(final NewFlowReceiver<K> newFlowReceiver, final ByteSerializerDeserializer<K> keySerDeser, final ByteSerializerDeserializer<V> valueSerDeser, final AbstractHashFunction<K> hashingStrategy, final UpdateStrategy<K, V> updateStrategy) throws IOException {
		super(keySerDeser, valueSerDeser, hashingStrategy, updateStrategy);
		setNewFlowRecevier(newFlowReceiver);
		newFlowReceiver.prepareToAppend();
	}

	@Override
	public boolean enqueue(K key, V value) throws IOException {
		newFlowReceiver.append(0, key);
		return false;
	}

	@Override
	public void close() throws IOException {
		newFlowReceiver.noMoreAppend();
	}

	@Override
	public void flush() throws IOException, InterruptedException {}
}
