package it.unimi.di.law.warc.processors;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.http.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

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

// RELEASE-STATUS: DIST

import it.unimi.di.law.warc.filters.Filter;
import it.unimi.di.law.warc.filters.parser.FilterParser;
import it.unimi.di.law.warc.io.CompressedWarcCachingReader;
import it.unimi.di.law.warc.io.WarcFormatException;
import it.unimi.di.law.warc.io.WarcReader;
import it.unimi.di.law.warc.records.WarcRecord;
import it.unimi.di.law.warc.util.ReorderingBlockingQueue;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;

public class ParallelFilteredProcessorRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(ParallelFilteredProcessorRunner.class);

	// TODO: document very clearly that no exception can be thrown by a processor, not even a runtime.
	public interface Processor<T> extends Closeable, FlyweightPrototype<Processor<T>> {
		public T process(final WarcRecord r, final long storePosition);
	}

	public interface Writer<T> extends Closeable {
		public void write(final T processedRecord, final long storePosition, final PrintStream out) throws IOException;
	}

	private static final class Result<T> {
		public static final Result<?>[] END_OF_RESULTS = new Result[0];
		private final T result;
		private final Writer<? super T> writer;
		private final PrintStream out;
		private final long storePosition;
		private Result(final T result, final Writer<? super T> writer, final long storePosition, final PrintStream out) {
			this.result = result;
			this.writer = writer;
			this.storePosition = storePosition;
			this.out = out;
		}
		private void write() throws IOException {
			this.writer.write(result, storePosition, out);
		}
	}

	private static class Step<T> implements FlyweightPrototype<Step<T>> {
		private final Processor<T> processor;
		private final Writer<? super T> writer;
		private final PrintStream out;
		private Step(final Processor<T> processor, final Writer<? super T> writer, final PrintStream out) {
			this.processor = processor;
			this.writer = writer;
			this.out = out;
		}
		private Result<T> run(final WarcReader reader, final long storePosition) throws Exception {
			Args.notNull(reader, "reader");
			final T result = processor.process(reader.read(), storePosition);
			if (result == null) return null;
			return new Result<>(result, this.writer, storePosition, this.out);
		}
		@Override
		public Step<T> copy() {
			return new Step<>(processor.copy(), writer, out);
		}
	}

	private final ObjectArrayList<Step<?>> steps = new ObjectArrayList<>();
	private final InputStream in;
	private final Filter<WarcRecord> filter;
	private ReorderingBlockingQueue<Result<?>[]> queue;
	protected volatile IOException flushingThreadException;

	public ParallelFilteredProcessorRunner(final InputStream in, final Filter<WarcRecord> filter) {
		this.in = in;
		this.filter = filter;
	}

	public ParallelFilteredProcessorRunner(final InputStream in) {
		this(in, null);
	}

	private static ObjectArrayList<Step<?>> copySteps(ObjectArrayList<Step<?>> steps) {
		final ObjectArrayList<Step<?>> copy = new ObjectArrayList<>(steps.size());
		for(final Step<?> step: steps) copy.add(step.copy());
		return copy;
	}

	public <T> ParallelFilteredProcessorRunner add(final Processor<T> p, final Writer<? super T> s, final PrintStream out) {
		steps.add(new Step<>(p, s, out));
		return this;
	}

	public void runSequentially() throws WarcFormatException, Exception {
		WarcReader r;
		final CompressedWarcCachingReader reader = new CompressedWarcCachingReader(in);
		final ProgressLogger pl = new ProgressLogger(LOGGER, 1, TimeUnit.MINUTES, "records");

		pl.itemsName = "records";
		pl.displayFreeMemory = true;
		pl.displayLocalSpeed = true;
		pl.start("Scanning...");

		for(long storePosition = 0; (r = reader.cache()) != null; storePosition++) {
			final WarcRecord record = r.read();
			if (filter == null || (filter != null && filter.apply(record)))
				for (final Step<?> s : steps) {
					final Result<?> result = s.run(r, storePosition);
					if (result != null) result.write();
				}
			pl.lightUpdate();
		}

		pl.done();

		for (final Step<?> s : steps) {
			s.processor.close();
			s.writer.close();
		}
	}

	private final class FlushingThread implements Callable<Void> {
		@Override
		public Void call() throws Exception {
			final ProgressLogger pl = new ProgressLogger(LOGGER, 1, TimeUnit.MINUTES, "records");

			pl.itemsName = "records";
			pl.displayFreeMemory = true;
			pl.displayLocalSpeed = true;
			pl.start("Scanning...");

			try {
				for(;;) {
					final Result<?>[] result = queue.take();
					if (result == Result.END_OF_RESULTS) {
						pl.done();
						return null;
					}
					for (final Result<?> r : result) if (r != null) r.write();
					pl.lightUpdate();
				}
			}
			catch(final Exception e) {
				LOGGER.error("Exception in flushing thread", e);
				throw e;
			}
		}
	}

	public void run() throws WarcFormatException, IOException, InterruptedException {
		run(Runtime.getRuntime().availableProcessors());
	}

	public void run(final int numberOfThreads) throws WarcFormatException, IOException, InterruptedException {
		queue = new ReorderingBlockingQueue<>(2 * numberOfThreads);

		final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setPriority(Thread.MAX_PRIORITY).setNameFormat("FlushingThread").build());
		final Future<Void> future = singleThreadExecutor.submit(new FlushingThread());

		final ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads, new ThreadFactoryBuilder().setNameFormat("ProcessingThread-%d").build());
		final ExecutorCompletionService<Void> executorCompletionService = new ExecutorCompletionService<>(executorService);

		final MutableLong counter = new MutableLong();
		final MutableBoolean end = new MutableBoolean();

		final Result<?>[] nullResult = new Result<?>[steps.size()];
		for (int i = nullResult.length - 1; i >= 0; i--) nullResult[i] = null;

		for(int i = numberOfThreads; i-- != 0;) executorCompletionService.submit(new Callable<Void>() {
			private final ObjectArrayList<Step<?>> stepsCopy = copySteps(ParallelFilteredProcessorRunner.this.steps);
			private final CompressedWarcCachingReader reader = new CompressedWarcCachingReader(in);
			private final Filter<WarcRecord> filterCopy = filter == null ? null : filter.copy();
			@Override
			public Void call() throws Exception {
				for(;;) {
					final long storePosition;
					WarcReader r = null;
					synchronized(counter) {
						if (end.booleanValue()) return null;
						try {
							r = reader.cache();
						}
						catch (final Exception e) {
							// We just log exceptions
							// TODO: make this configurable
							LOGGER.error("Exception while reading store", e);
						}
						if (r == null) {
							end.setValue(true);
							return null;
						}
						storePosition = counter.longValue();
						counter.increment();
					}
					final Result<?>[] result;
					boolean passFilter = true;
					if (filterCopy != null) {
						final WarcRecord record = r.read();
						if (! filterCopy.apply(record)) passFilter = false;
					}
					if (passFilter) {
						result = new Result<?>[stepsCopy.size()];
						int i = 0;
						for (final Step<?> s : stepsCopy) result[i++] = s.run(r, storePosition);
					} else
						result = nullResult;
					queue.put(result, storePosition);
				}
			}
		});

		// We have to be a bit robust w.r.t. stores with problems at the end.
		@SuppressWarnings("unused")
		Throwable readingProblem = null; // TODO: throw it conditionally to an option
		for(int i = numberOfThreads; i-- != 0;)
			try {
				executorCompletionService.take().get();
			}
			catch(final Exception e) {
				LOGGER.error("Unexpected exception in parallel thread", e.getCause());
				readingProblem = e.getCause(); // We keep only the last one. They will be logged anyway.
			}

		executorService.shutdown();

		try {
			queue.put(Result.END_OF_RESULTS, counter.longValue());
			future.get();
		}
		catch(final ExecutionException e) {
			final Throwable cause = e.getCause();
			throw cause instanceof RuntimeException ? (RuntimeException)cause : new RuntimeException(cause.getMessage(), cause);
		}
		finally {
			singleThreadExecutor.shutdown();
		}

		for (final Step<?> s : steps) {
			s.processor.close();
			s.writer.close();
		}
	}

	public static void main(final String[] arg) throws Exception {
		final SimpleJSAP jsap = new SimpleJSAP(ParallelFilteredProcessorRunner.class.getName(), "Processes a store.",
				new Parameter[] {
				new FlaggedOption("filter", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "filter", "A WarcRecord filter that recods must pass in order to be processed."),
		 		new FlaggedOption("processor", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'p', "processor", "A processor to be applied to data.").setAllowMultipleDeclarations(true),
			 	new FlaggedOption("writer", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'w', "writer", "A writer to be applied to the results.").setAllowMultipleDeclarations(true),
				new FlaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o', "output", "The output filename  (- for stdout).").setAllowMultipleDeclarations(true),
				new FlaggedOption("threads", JSAP.INTSIZE_PARSER, Integer.toString(Runtime.getRuntime().availableProcessors()), JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used."),
				new Switch("sequential", 'S', "sequential"),
				new UnflaggedOption("store", JSAP.STRING_PARSER, JSAP.NOT_REQUIRED, "The name of the store (if omitted, stdin)."),
		});

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) return;

		final String filterSpec = jsapResult.getString("filter");
		final Filter<WarcRecord> filter;
		if (filterSpec != null) {
			final FilterParser<WarcRecord> parser = new FilterParser<>(WarcRecord.class);
			filter = parser.parse(filterSpec);
		} else
			filter = null;
		final InputStream in = jsapResult.userSpecified("store") ? new FastBufferedInputStream(new FileInputStream(jsapResult.getString("store"))) : System.in;
		final ParallelFilteredProcessorRunner parallelFilteredProcessorRunner = new ParallelFilteredProcessorRunner(in, filter);

		final String[] processor =  jsapResult.getStringArray("processor");
		final String[] writer =  jsapResult.getStringArray("writer");
		final String[] output =  jsapResult.getStringArray("output");
		if (processor.length != writer.length) throw new IllegalArgumentException("You must specify the same number or processors and writers");
		if (output.length != writer.length) throw new IllegalArgumentException("You must specify the same number or output specifications and writers");

		final String[] packages = new String[] { ParallelFilteredProcessorRunner.class.getPackage().getName() };
		final PrintStream[] ops = new PrintStream[processor.length];
		for (int i = 0; i < processor.length; i++) {
			ops[i] = "-".equals(output[i]) ? System.out : new PrintStream(new FastBufferedOutputStream(new FileOutputStream(output[i])), false, "UTF-8");
			// TODO: these casts to SOMETHING<Object> are necessary for compilation under Eclipse. Check in the future.
			parallelFilteredProcessorRunner.add((Processor<Object>)ObjectParser.fromSpec(processor[i], Processor.class, packages, new String[] { "getInstance" }),
					(Writer<Object>)ObjectParser.fromSpec(writer[i], Writer.class,  packages, new String[] { "getInstance" }),
					ops[i]);
		}

		if (jsapResult.userSpecified("sequential")) parallelFilteredProcessorRunner.runSequentially();
		else parallelFilteredProcessorRunner.run(jsapResult.getInt("threads"));

		for (int i = 0; i < processor.length; i++) ops[i].close();

	}
}
