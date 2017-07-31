package it.unimi.di.law.bubing.parser;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.warc.filters.URIResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.NoSuchAlgorithmException;

import org.apache.http.HttpResponse;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

// RELEASE-STATUS: DIST

/** A universal binary parser that just computes digests. */
public class BinaryParser implements Parser<Void> {
	private final HashFunction hashFunction;
	private byte[] buffer;
	private boolean crossAuthorityDuplicates;

	/** Return the hash function corresponding to a given message-digest algorithm given by name.
	 *
	 * @param messageDigest a message-digest algorithm (e.g., <code>MurmurHash3</code> or <code>MD5</code>); {@code null} if {@code messageDigest} is the empty string.
	 */
	@SuppressWarnings("deprecation")
	public final static HashFunction forName(final String messageDigest) throws NoSuchAlgorithmException {
		if ("".equals(messageDigest)) return null;
		if ("MD5".equalsIgnoreCase(messageDigest)) return Hashing.md5();
		if ("MurmurHash3".equalsIgnoreCase(messageDigest)) return Hashing.murmur3_128();
		throw new NoSuchAlgorithmException("Unknown hash function " + messageDigest);
	}


	/** Builds a parser for digesting a page.
	 *
	 * @param hashFunction the hash function used to digest, {@code null} if no digesting will be performed.
	 */
	public BinaryParser(final HashFunction hashFunction) {
		this(hashFunction, false);
	}

	public BinaryParser(final HashFunction hashFunction, final boolean crossAuthorityDuplicates) {
		this.hashFunction = hashFunction;
		this.buffer = new byte[1024];
		this.crossAuthorityDuplicates = crossAuthorityDuplicates;
	}

	/** Builds a parser for digesting a page.
	 *
	 * @param messageDigestAlgorithm the digesting algorithm (as a string).
	 */
	public BinaryParser(final String messageDigestAlgorithm) throws NoSuchAlgorithmException {
		 this(forName(messageDigestAlgorithm));
	}

	public Hasher init(final URI url) {
		final Hasher hasher = hashFunction.newHasher();
		if (url != null) {
			// Note that we need to go directly to the hasher to encode explicit IP addresses
			hasher.putUnencodedChars(url.getHost());
			hasher.putByte((byte)0);
		}
		return hasher;
	}

	@Override
	public byte[] parse(final URI uri, final HttpResponse httpResponse, final LinkReceiver linkReceiver) throws IOException {
		if (hashFunction == null) return null;
		final InputStream is = httpResponse.getEntity().getContent();
		final Hasher hasher = init(crossAuthorityDuplicates? null : uri);
		for(int length; (length = is.read(buffer, 0, buffer.length)) > 0;) hasher.putBytes(buffer, 0, length);
		return hasher.hash().asBytes();
	}

	@Override
	public boolean apply(URIResponse response) {
		return true;
	}

	@Override
	public Object clone() {
		return new BinaryParser(hashFunction);
	}

	@Override
	public String guessedCharset() {
		return null;
	}

	@Override
	public BinaryParser copy() {
		return new BinaryParser(hashFunction, crossAuthorityDuplicates);
	}

	@Override
	public Void result() {
		return null;
	}
}
