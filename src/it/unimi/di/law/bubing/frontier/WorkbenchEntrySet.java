package it.unimi.di.law.bubing.frontier;

/*
 * Copyright (C) 2012-2017 Sebastiano Vigna
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
import it.unimi.dsi.fastutil.Hash;

import java.util.Arrays;

//RELEASE-STATUS: DIST

/** A data structure representing the set of {@linkplain WorkbenchEntry workbench entries} created so far.
 *  It is a lightweight implementation of a map from IP addresses to workbench entries.
 */
public class WorkbenchEntrySet implements java.io.Serializable, Hash {
	private static final long serialVersionUID = 0L;

	/** The array of keys. */
	protected transient WorkbenchEntry[] workbenchEntry;
	/** The current table size. */
	protected transient int n;
	/** The mask for wrapping a position counter. */
	protected transient int mask;
	/** Number of entries in the set. */
	protected int size;
	/** The maximum number of entries that can be filled before rehashing. */
	protected int maxFill;

	/** Creates a set of workbench entries. */
	public WorkbenchEntrySet() {
		n = 1024;
		mask = n - 1;
		maxFill = 3 * (n / 4);
		workbenchEntry = new WorkbenchEntry[n];
	}

	/** Returns the array of workbench entries; the order is arbitrary. The array may contain {@code null} elements.
	 *
	 * @return the array of workbench entries; it may contain {@code null} entries.
	 */
	public WorkbenchEntry[] workbenchEntries() {
		return workbenchEntry;
	}

	/** Ensures that the set has a given capacity.
	 *
	 * @param capacity the desired capacity.
	 */
	public void ensureCapacity(final int capacity) {
		rehash(arraySize(capacity, (float)(3./4)));
	}

	// TODO: change with something better
	private final static int hashCode(final byte[] a) {
		return it.unimi.dsi.fastutil.HashCommon.murmurHash3(Arrays.hashCode(a));
	}

	/** Adds a workbench entry to the set, if necessary.
	 *
	 * @param e the entry to be added.
	 * @return true if the state set changed as a result of this operation.
	 */
	public boolean add(final WorkbenchEntry e) {
		// The starting point.
		int pos = hashCode(e.ipAddress) & mask;
		// There's always an unused entry.
		while (workbenchEntry[pos] != null) {
			if (Arrays.equals(workbenchEntry[pos].ipAddress, e.ipAddress)) return false;
			pos = (pos + 1) & mask;
		}
		workbenchEntry[pos] = e;
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
			while (workbenchEntry[pos] != null) {
				slot = hashCode(workbenchEntry[pos].ipAddress) & mask;
				if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) break;
				pos = (pos + 1) & mask;
			}
			if (workbenchEntry[pos] == null) break;
			workbenchEntry[last] = workbenchEntry[pos];
		}
		workbenchEntry[last] = null;
		return last;
	}

	/** Removes a given workbench entry.
	 *
	 * @param e the workbench entry to be removed.
	 * @return true if the state set changed as a result of this operation.
	 */
	public boolean remove(final WorkbenchEntry e) {
		// The starting point.
		int pos = hashCode(e.ipAddress) & mask;
		// There's always an unused entry.
		while (workbenchEntry[pos] != null) {
			if (workbenchEntry[pos] == e) {
				size--;
				shiftKeys(pos);
				// TODO: implement resize
				return true;
			}
			pos = (pos + 1) & mask;
		}
		return false;
	}

	/** Returns the entry for a given IP address.
	 *
	 * @param address the IP address.
	 * @return the workbench entry corresponding to a given address, or {@code null}.
	 */
	public WorkbenchEntry get(final byte[] address) {
		// The starting point.
		int pos = hashCode(address) & mask;
		// There's always an unused entry.
		while (workbenchEntry[pos] != null) {
			if (Arrays.equals(workbenchEntry[pos].ipAddress, address)) return workbenchEntry[pos];
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
		Arrays.fill(workbenchEntry, null);
	}

	/** Returns the size (number of entries) in the workbench.
	 *
	 * @return the size (number of entries) in the workbench.
	 */
	public int size() {
		return size;
	}

	/** Returns whether the workbench is empty.
	 *
	 * @return whether the workbench is empty.
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	/** Rehashes the set to a new size.
	 *
	 * @param newN the new size.
	 */
	protected void rehash(final int newN) {
		int i = 0, pos;
		final WorkbenchEntry[] workbenchEntry = this.workbenchEntry;
		final int newMask = newN - 1;
		final WorkbenchEntry[] newWorkbenchEntry = new WorkbenchEntry[newN];

		for (int j = size; j-- != 0;) {
			while (workbenchEntry[i] == null) i++;
			WorkbenchEntry e = workbenchEntry[i];
			pos = hashCode(e.ipAddress) & newMask;
			while (newWorkbenchEntry[pos] != null) pos = (pos + 1) & newMask;
			newWorkbenchEntry[pos] = e;
			i++;
		}
		n = newN;
		mask = newMask;
		maxFill = 3 * (n / 4);
		this.workbenchEntry = newWorkbenchEntry;
	}


	private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
		s.defaultWriteObject();
		for(int i = workbenchEntry.length; i-- != 0;) if (workbenchEntry[i] != null)  s.writeObject(workbenchEntry[i]);
	}

	private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
		s.defaultReadObject();
		n = arraySize(size, (float)(3./4));
		maxFill = 3 * (n / 4);
		mask = n - 1;
		final WorkbenchEntry[] workbenchEntry = this.workbenchEntry = new WorkbenchEntry[n];
		for (int i = size, pos = 0; i-- != 0;) {
			WorkbenchEntry e = (WorkbenchEntry)s.readObject();
			pos = hashCode(e.ipAddress) & mask;
			while (workbenchEntry[pos] != null) pos = (pos + 1) & mask;
		}
	}
}
