package it.unimi.di.law.bubing.parser;

/*
 * Copyright (C) 2004-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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
