package it.unimi.di.law.bubing.sieve;

/*
 * Copyright (C) 2010-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
