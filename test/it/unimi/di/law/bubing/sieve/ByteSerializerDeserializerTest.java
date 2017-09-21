package it.unimi.di.law.bubing.sieve;

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
