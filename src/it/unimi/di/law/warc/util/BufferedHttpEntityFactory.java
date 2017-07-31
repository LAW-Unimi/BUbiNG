package it.unimi.di.law.warc.util;

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

// RELEASE-STATUS: DIST

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.entity.BufferedHttpEntity;

/** An implementation of a {@link HttpEntityFactory} that returns a {@link BufferedHttpEntity}. */
public class BufferedHttpEntityFactory implements HttpEntityFactory {

	public static final BufferedHttpEntityFactory INSTANCE = new BufferedHttpEntityFactory();

	private BufferedHttpEntityFactory() {}

	@Override
	public HttpEntity newEntity(final HttpEntity from) throws IOException {
		return new BufferedHttpEntity(from);
	}

}
