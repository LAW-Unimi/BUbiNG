package it.unimi.di.law.bubing.frontier;

/*
 * Copyright (C) 2013-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import it.unimi.di.law.bubing.util.MurmurHash3;
import it.unimi.dsi.fastutil.Hash;

import java.util.Arrays;

//RELEASE-STATUS: DIST

/** A data structure representing the set of {@linkplain VisitState visit states} created so far.
 *  It is a lightweight implementation of a map from scheme+authorities to visit states.
 */
public class VisitStateSet implements java.io.Serializable, Hash {
	private static final long serialVersionUID = 0L;

	/** The array of keys. */
	protected transient VisitState[] visitState;
	/** The current table size. */
	protected transient int n;
	/** The mask for wrapping a position counter. */
	protected transient int mask;
	/** Number of entries in the set. */
	protected int size;
	/** The maximum number of entries that can be filled before rehashing. */
	protected int maxFill;

	/** Creates an empty visit state set. */
	public VisitStateSet() {
		n = 1024;
		mask = n - 1;
		maxFill = 3 * (n / 4);
		visitState = new VisitState[n];
	}

	/** Returns the array of visit states; the order is arbitrary. The array may contain {@code null} elements.
	 *
	 * @return the array of visit states; it may contain {@code null} entries.
	 */
	public VisitState[] visitStates() {
		return visitState;
	}

	/** Ensures that the set has a given capacity.
	 *
	 * @param capacity the desired capacity.
	 */
	public void ensureCapacity(final int capacity) {
		rehash(arraySize(capacity, (float)(3./4)));
	}

	/** Adds a visit state to the set, if necessary.
	 *
	 * @param v the state to be added.
	 * @return true if the state set changed as a result of this operation.
	 */
	public boolean add(final VisitState v) {
		// The starting point.
		int pos = (int)(MurmurHash3.hash(v.schemeAuthority) & mask);
		// There's always an unused entry.
		while (visitState[pos] != null) {
			if (Arrays.equals(visitState[pos].schemeAuthority, v.schemeAuthority)) return false;
			pos = (pos + 1) & mask;
		}
		visitState[pos] = v;
		if (++size >= maxFill && n < (1 << 30)) rehash(2 * n);
		return true;
	}

	/** Shifts left entries with the specified hash code, starting at the specified position, and
	 * empties the resulting free entry.
	 *
	 * @param pos a starting position.
	 * @return the position cleared by the shifting process. */
	protected final int shiftKeys(int pos) {
		// Shift entries with the same hash.
		int last, slot;
		for (;;) {
			pos = ((last = pos) + 1) & mask;
			while (visitState[pos] != null) {
				slot = (int)(MurmurHash3.hash(visitState[pos].schemeAuthority) & mask);
				if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) break;
				pos = (pos + 1) & mask;
			}
			if (visitState[pos] == null) break;
			visitState[last] = visitState[pos];
		}
		visitState[last] = null;
		return last;
	}

	/** Removes a given visit state.
	 *
	 * @param k the visit state to be removed.
	 * @return true if the state set changed as a result of this operation.
	 */
	public boolean remove(final VisitState k) {
		// The starting point.
		int pos = (int)(MurmurHash3.hash(k.schemeAuthority) & mask);
		// There's always an unused entry.
		while (visitState[pos] != null) {
			if (visitState[pos] == k) {
				size--;
				shiftKeys(pos);
				// TODO: implement resize
				return true;
			}
			pos = (pos + 1) & mask;
		}
		return false;
	}

	/** Checks that the content of an array is equal to the content of an array fragment, assuming they have the same length (otherwise, behavior will be impredictable).
	 *
	 * @param a an array.
	 * @param b another array.
	 * @param offset the first valid byte in {@code b}.
	 * @param length the number of valid elements in {@code b}.
	 * @return true of the content of {@code a} is equal to the content of the specified fragment of {@code b}.
	 */
	private static final boolean equals(final byte[] a, final byte[] b, final int offset, int length) {
		while(length-- != 0) if (a[length] != b[offset + length]) return false;
		return true;
	}

	/** Returns the visit state associated to a given scheme+authority, or {@code null}.
	 *
	 * @param array a byte array.
	 * @return the visit state associated to a given scheme+authority, or {@code null}.
	 */
	public VisitState get(final byte[] array) {
		// The starting point.
		int pos = (int)(MurmurHash3.hash(array) & mask);
		// There's always an unused entry.
		while (visitState[pos] != null) {
			if (Arrays.equals(visitState[pos].schemeAuthority, array)) return visitState[pos];
			pos = (pos + 1) & mask;
		}
		return null;
	}



	/** Returns the visit state associated to a given scheme+authority specified as a byte-array fragment, or {@code null}.
	 *
	 * @param array a byte array.
	 * @param offset the first valid byte in {@code array}.
	 * @param length the number of valid elements in {@code array}.
	 * @return the visit state associated to a given scheme+authority, or {@code null}.
	 */
	public VisitState get(final byte[] array, final int offset, final int length) {
		// The starting point.
		int pos = (int)(MurmurHash3.hash(array, offset, length) & mask);
		// There's always an unused entry.
		while(visitState[pos] != null) {
			if (visitState[pos].schemeAuthority.length == length && equals(visitState[pos].schemeAuthority, array, offset, length)) return visitState[pos];
			pos = (pos + 1) & mask;
		}
		return null;
	}

	/** Removes all elements from this set.
	 *
	 * <P>To increase object reuse, this method does not change the table size.
	 */
	public void clear() {
		if (size == 0) return;
		size = 0;
		Arrays.fill(visitState, null);
	}

	/** The number of visit states.
	 *
	 * @return the number of visit states.
	 */
	public int size() {
		return size;
	}

	/** Returns whether the set is empty.
	 *
	 * @return whether the set is empty.
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	/** Rehashes the state set to a new size.
	 *
	 * @param newN the new size.
	 */
	protected void rehash(final int newN) {
		int i = 0, pos;
		final VisitState[] visitState = this.visitState;
		final int newMask = newN - 1;
		final VisitState[] newVisitState = new VisitState[newN];

		for (int j = size; j-- != 0;) {
			while (visitState[i] == null) i++;
			VisitState v = visitState[i];
			pos = (int)(MurmurHash3.hash(v.schemeAuthority) & newMask);
			while (newVisitState[pos] != null) pos = (pos + 1) & newMask;
			newVisitState[pos] = v;
			i++;
		}
		n = newN;
		mask = newMask;
		maxFill = 3 * (n / 4);
		this.visitState = newVisitState;
	}


	private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
		s.defaultWriteObject();
		for(int i = visitState.length; i-- != 0;) if (visitState[i] != null)  s.writeObject(visitState[i]);
	}

	private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
		s.defaultReadObject();
		n = arraySize(size, (float)(3./4));
		maxFill = 3 * (n / 4);
		mask = n - 1;
		final VisitState[] visitState = this.visitState = new VisitState[n];
		for (int i = size, pos = 0; i-- != 0;) {
			VisitState v = (VisitState)s.readObject();
			pos = (int)(MurmurHash3.hash(v.schemeAuthority) & mask);
			while (visitState[pos] != null) pos = (pos + 1) & mask;
		}
	}
}
