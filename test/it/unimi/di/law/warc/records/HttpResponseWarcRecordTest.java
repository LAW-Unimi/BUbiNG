package it.unimi.di.law.warc.records;

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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.bubing.util.FetchDataTest;
import it.unimi.di.law.bubing.util.Helpers;
import it.unimi.di.law.warc.io.UncompressedWarcWriter;
import it.unimi.di.law.warc.io.WarcWriter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

public class HttpResponseWarcRecordTest {

	//TODO: this is not actually a test, it's just a main (shame on me)


	public static void main(String[] arg) throws JSAPException, URISyntaxException, NoSuchAlgorithmException, ClientProtocolException, IOException, InterruptedException, ConfigurationException, IllegalArgumentException, ClassNotFoundException {

		SimpleJSAP jsap = new SimpleJSAP(HttpResponseWarcRecordTest.class.getName(), "Outputs an URL (given as argument) as the UncompressedWarcWriter would do",
			new Parameter[] {
				new UnflaggedOption("url", JSAP.STRING_PARSER, JSAP.REQUIRED, "The url of the page."),
			});

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String url = jsapResult.getString("url");

		final URI uri = new URI(url);
		final WarcWriter writer = new UncompressedWarcWriter(System.out);

		// Setup FetchData
		final RuntimeConfiguration testConfiguration = Helpers.getTestConfiguration(null);
		final HttpClient httpClient = FetchDataTest.getHttpClient(null, false);
		final FetchData fetchData = new FetchData(testConfiguration);

		fetchData.fetch(uri, httpClient, null, null, false);
		final HttpResponseWarcRecord record = new HttpResponseWarcRecord(uri, fetchData.response());
		writer.write(record);
		fetchData.close();
		System.out.println(record);

		writer.close();
	}

}
