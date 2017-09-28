package it.unimi.di.law.bubing.store;

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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.warc.io.ParallelBufferedWarcWriter;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.Date;
import java.text.SimpleDateFormat;


import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpResponse;
import org.apache.http.message.HeaderGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//RELEASE-STATUS: DIST

/** A {@link Store} implementation using the {@link it.unimi.di.law.warc} package. */

public class MultiWarcStore implements Closeable, Store {
	private final static Logger LOGGER = LoggerFactory.getLogger( WarcStore.class );

	public final int OUTPUT_STREAM_BUFFER_SIZE = 1024 * 1024;
	public final static String STORE_NAME_FORMAT = "store.warc.%s.%s.gz";
	public final static String DIGESTS_NAME = "digests.bloom";
	public final static int NUM_GZ_WARC_RECORDS = 16;
	private int maxRecordsPerFile = 25600;
	private int maxSecondsBetweenDumps = 600;
	private int currentNumberOfRecordsInFile = 0;
	private long lastDumpTime = (new Date()).getTime()/1000;
	private Object counterLock = new Object();
	private FastBufferedOutputStream warcOutputStream;
	private ParallelBufferedWarcWriter warcWriter;

	private final File storeDir;

	public MultiWarcStore( final RuntimeConfiguration rc ) throws IOException {
		storeDir = rc.storeDir;
		maxRecordsPerFile = rc.maxRecordsPerFile;
		maxSecondsBetweenDumps = rc.maxSecondsBetweenDumps;
		LOGGER.info("Max record per file = " + maxRecordsPerFile);
		LOGGER.info("Max seconds between dumps = " + maxSecondsBetweenDumps);
		createNewWriter( );	
	}
	private String generateStoreName(Date d) {
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss.SSS");
        String datetime = ft.format(d);
		return String.format(STORE_NAME_FORMAT, datetime, UUID.randomUUID());
	}


	private void createNewWriter() throws IOException {
		Date now = new Date();
		final File warcFile = new File( storeDir, generateStoreName(now) );
		warcOutputStream = new FastBufferedOutputStream( new FileOutputStream( warcFile ), OUTPUT_STREAM_BUFFER_SIZE );
		warcWriter = new ParallelBufferedWarcWriter(warcOutputStream, true );
	}
		
	@Override
	public void store( final URI uri, final HttpResponse response, final boolean isDuplicate, final byte[] contentDigest, final String guessedCharset ) throws IOException, InterruptedException {
		
		if ( contentDigest == null ) throw new NullPointerException( "Content digest is null" );
		final HttpResponseWarcRecord record = new HttpResponseWarcRecord( uri, response );
		HeaderGroup warcHeaders = record.getWarcHeaders();
		warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.WARC_PAYLOAD_DIGEST, "bubing:" + Hex.encodeHexString( contentDigest ) ) );
		if ( guessedCharset != null ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_GUESSED_CHARSET, guessedCharset ) );
		if ( isDuplicate ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_IS_DUPLICATE, "true" ) );
		synchronized(counterLock) {
			long currentTime = new Date().getTime()/1000;
			
			if (currentNumberOfRecordsInFile > 0 && ((currentNumberOfRecordsInFile > maxRecordsPerFile) || 
				(currentTime-lastDumpTime > maxSecondsBetweenDumps))) {
				LOGGER.info("Current time = " + currentTime + ", lastDumpTime = " + lastDumpTime );
				currentNumberOfRecordsInFile = 0;
				lastDumpTime = currentTime;
				LOGGER.warn( "Target number of records reached, creating new output file" );
				try {
					warcWriter.close();
				} catch ( IOException e ) {
					LOGGER.error( "Closing interrupted");
				}
				warcOutputStream.close();
				createNewWriter();
			}
		}
		warcWriter.write( record );

		currentNumberOfRecordsInFile += 1;
	}
	
	@Override
	public synchronized void close() throws IOException {
		try {
			warcWriter.close();
		}
		catch ( IOException shouldntHappen ) {
			LOGGER.error( "Interrupted while closing parallel output stream" );
		}
		warcOutputStream.close();
	}
}
