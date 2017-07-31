package it.unimi.di.law.bubing.sieve;

/*
 * Copyright (C) 2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

//RELEASE-STATUS: DIST

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;


public class ByteSerializerDeserializerTest {

	@Test
	public void testINTEGER() throws IOException {
		Random r = new Random(0);
		for (int i = 0; i < 100; i++) {
			int x = r.nextInt();
			FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream();
			ByteSerializerDeserializer.INTEGER.toStream(Integer.valueOf(x), fbaos);
			Integer result = ByteSerializerDeserializer.INTEGER.fromStream(new FastByteArrayInputStream(fbaos.array));
			assertEquals(x, result.intValue());
		}
	}
}
