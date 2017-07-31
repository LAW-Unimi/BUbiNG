package it.unimi.di.law.bubing.parser;

/*
 * Copyright (C) 2004-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.bubing.parser.Parser.TextProcessor;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.shorts.Short2ShortOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.commons.io.input.CharSequenceReader;

// RELEASE-STATUS: DIST

/** An implementation of a {@link Parser.TextProcessor} that accumulates the counts of terms from a given set specified via a
 * {@link StringMap}. */
public final class SpamTextProcessor implements TextProcessor<SpamTextProcessor.TermCount> {
	public final static class TermCount extends Short2ShortOpenHashMap {
		private static final long serialVersionUID = 1L;
	};

	private final FastBufferedReader fbr = new FastBufferedReader();
	private final TermCount termCount = new TermCount();
	private final Object2LongFunction<MutableString> termSetOnthology;

	public SpamTextProcessor(Object2LongFunction<MutableString> termSetOnthology) {
		this.termSetOnthology = termSetOnthology;
	}

	@SuppressWarnings("unchecked")
	public SpamTextProcessor(final String termSetOnthologyURI) throws ClassNotFoundException, MalformedURLException, IOException {
		 termSetOnthology = (Object2LongFunction<MutableString>)BinIO.loadObject(new URL(termSetOnthologyURI).openStream());
	}

	private void process() throws IOException {
		final MutableString word = new MutableString(), nonWord = new MutableString();
		while (fbr.next(word, nonWord)) {
			final short index = (short)termSetOnthology.getLong(word.toLowerCase());
			if (index != -1) {
				final short oldValue = termCount.get(index);
				if (oldValue < Short.MAX_VALUE) termCount.put(index, (short)(oldValue + 1));
			}
		}
	}

	@Override
	public Appendable append(CharSequence csq) throws IOException {
		fbr.setReader(new CharSequenceReader(csq));
		process();
		return this;
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end) throws IOException {
		fbr.setReader(new CharSequenceReader(csq.subSequence(start, end)));
		process();
		return this;
	}

	@Override
	public Appendable append(char c) throws IOException {
		final short index = (short)termSetOnthology.getLong(new MutableString().append(Character.toLowerCase(c)));
		if (index != -1) {
			final short oldValue = termCount.get(index);
			if (oldValue < Short.MAX_VALUE) termCount.put(index, (short)(oldValue + 1));
		}

		return this;
	}

	@Override
	public void init(URI responseUrl) {
		termCount.clear();
	}

	@Override
	public TermCount result() {
		return termCount;
	}

	@Override
	public TextProcessor<TermCount> copy() {
		return new SpamTextProcessor(termSetOnthology);
	}
}
