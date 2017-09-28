package it.unimi.di.law.warc.records;

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
