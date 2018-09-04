/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.sort;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExceptionHandler;
import org.apache.flink.runtime.operators.sort.IndexedSorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.operators.sort.SortedDataFile;
import org.apache.flink.runtime.operators.sort.Sorter;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.table.util.BinaryMergeIterator;
import org.apache.flink.table.util.ChannelWithMeta;
import org.apache.flink.table.util.MemUtil;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.runtime.sort.BinaryExternalMerger.getSegmentsForReaders;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link BinaryExternalSorter} is a full fledged sorter. It implements a multi-way merge sort.
 * Internally, it has two asynchronous threads (sort, spill) which communicate through a set of
 * blocking circularQueues, forming a closed loop.  Memory is allocated using the
 * {@link MemoryManager} interface. Thus the component will not exceed the provided memory limits.
 */
public class BinaryExternalSorter implements Sorter<BinaryRow> {

	// ------------------------------------------------------------------------
	//                              Constants
	// ------------------------------------------------------------------------

	/** The minimal number of buffers to use by the writers. */
	protected static final int MIN_NUM_WRITE_BUFFERS = 2;

	/** The maximal number of buffers to use by the writers. */
	protected static final int MAX_NUM_WRITE_BUFFERS = 4;

	/** The minimum number of segments that are required for the sort to operate. */
	protected static final int MIN_NUM_SORT_MEM_SEGMENTS = 10;

	public static final long SORTER_MIN_NUM_SORT_MEM =
			(MIN_NUM_SORT_MEM_SEGMENTS + MIN_NUM_WRITE_BUFFERS) * MemoryManager.DEFAULT_PAGE_SIZE;

	/** Logging. */
	private static final Logger LOG = LoggerFactory.getLogger(BinaryExternalSorter.class);

	// ------------------------------------------------------------------------
	//                                  Threads
	// ------------------------------------------------------------------------

	/**
	 * The currWriteBuffer that is passed as marker for the end of data.
	 */
	private static final CircularElement EOF_MARKER = new CircularElement();

	/**
	 * The currWriteBuffer that is passed as marker for signal beginning of spilling.
	 */
	private static final CircularElement SPILLING_MARKER = new CircularElement();

	// ------------------------------------------------------------------------
	//                                   Memory
	// ------------------------------------------------------------------------

	/**
	 * The memory segments used first for sorting and later for reading/pre-fetching
	 * during the external merge.
	 */
	protected final List<List<MemorySegment>> sortReadMemory;

	/**
	 * Records all sort buffer.
	 */
	protected final List<BinaryInMemorySortBuffer> sortBuffers;

	protected final int fixedReadMemoryNum;

	/** The memory segments used to stage data to be written. */
	protected final List<MemorySegment> writeMemory;

	/** The memory manager through which memory is allocated and released. */
	protected final MemoryManager memoryManager;

	// ------------------------------------------------------------------------
	//                            Miscellaneous Fields
	// ------------------------------------------------------------------------

	/**
	 * The monitor which guards the iterator field.
	 */
	protected final Object iteratorLock = new Object();

	/** The thread that merges the buffer handed from the reading thread. */
	private ThreadBase sortThread;

	/** The thread that handles spilling to secondary storage. */
	private ThreadBase spillThread;

	/**
	 * Final result iterator.
	 */
	protected volatile MutableObjectIterator<BinaryRow> iterator;

	/**
	 * The exception that is set, if the iterator cannot be created.
	 */
	protected volatile IOException iteratorException;

	/**
	 * Flag indicating that the sorter was closed.
	 */
	protected volatile boolean closed;

	/**
	 * Sort or spill thread maybe occur some exceptions.
	 */
	private ExceptionHandler<IOException> exceptionHandler;

	/**
	 * Queue for the communication between the threads.
	 */
	private CircularQueues circularQueues;

	private long bytesUntilSpilling;

	private CircularElement currWriteBuffer;

	private boolean writingDone = false;

	private final Object writeLock = new Object();

	private final SpillChannelManager channelManager;

	private final BinaryExternalMerger merger;

	// ------------------------------------------------------------------------
	//                         Constructor & Shutdown
	// ------------------------------------------------------------------------

	private final BinaryRowSerializer serializer;

	//metric
	private long numSpillFiles;
	private long spillInBytes;

	public BinaryExternalSorter(
			final Object owner, MemoryManager memoryManager, long reservedMemorySize, long preferredMemorySize,
			long perRequestMemorySize, IOManager ioManager, TypeSerializer<BaseRow> inputSerializer,
			BinaryRowSerializer serializer, NormalizedKeyComputer normalizedKeyComputer,
			RecordComparator comparator) throws IOException {
		this(owner, memoryManager, reservedMemorySize, preferredMemorySize, perRequestMemorySize, ioManager,
				inputSerializer, serializer, normalizedKeyComputer, comparator,
				ConfigConstants.DEFAULT_SPILLING_MAX_FAN, ConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD);
	}

	public BinaryExternalSorter(
			final Object owner, MemoryManager memoryManager, long reservedMemorySize, long preferredMemorySize,
			long perRequestMemorySize, IOManager ioManager, TypeSerializer<BaseRow> inputSerializer,
			BinaryRowSerializer serializer,
			NormalizedKeyComputer normalizedKeyComputer,
			RecordComparator comparator, int maxNumFileHandles,
			float startSpillingFraction) throws IOException {
		checkArgument(maxNumFileHandles >= 2);
		checkNotNull(ioManager);
		checkNotNull(normalizedKeyComputer);
		checkNotNull(comparator);
		this.serializer = (BinaryRowSerializer) serializer.duplicate();
		this.memoryManager = checkNotNull(memoryManager);

		// adjust the memory quotas to the page size
		final int numMinPagesTotal = (int) (reservedMemorySize / memoryManager.getPageSize());

		if (reservedMemorySize < SORTER_MIN_NUM_SORT_MEM) {
			throw new IllegalArgumentException("Too little memory provided to sorter to perform task. " +
					"Required are at least " + (MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS) +
					" pages. Current page size is " + memoryManager.getPageSize() + " bytes.");
		}

		// determine how many buffers to use for writing
		final int numWriteBuffers;

		// determine how many buffers we have when we do a full mere with maximal fan-in
		final int minBuffersForMerging = maxNumFileHandles + MIN_NUM_WRITE_BUFFERS;

		if (minBuffersForMerging > numMinPagesTotal) {
			numWriteBuffers = MIN_NUM_WRITE_BUFFERS;
			maxNumFileHandles = numMinPagesTotal - MIN_NUM_WRITE_BUFFERS;
		} else {
			// we are free to choose. make sure that we do not eat up too much memory for writing
			final int fractionalAuxBuffers = numMinPagesTotal / (100);

			if (fractionalAuxBuffers >= MAX_NUM_WRITE_BUFFERS) {
				numWriteBuffers = MAX_NUM_WRITE_BUFFERS;
			} else {
				numWriteBuffers = Math.max(MIN_NUM_WRITE_BUFFERS, fractionalAuxBuffers); // at least the lower bound
			}
		}

		final int sortMemPages = numMinPagesTotal - numWriteBuffers;
		final long sortMemory = ((long) sortMemPages) * memoryManager.getPageSize();

		// decide how many sort buffers to use
		int numSortBuffers = 1;
		final long sortMaxMemSize = Math.max(preferredMemorySize, reservedMemorySize) -
				numWriteBuffers * memoryManager.getPageSize();
		if (sortMaxMemSize > 100 * 1024 * 1024L) {
			numSortBuffers = 2;
		}
		final int numSegmentsPerSortBuffer = sortMemPages / numSortBuffers;
		this.sortReadMemory = new ArrayList<>();
		List<MemorySegment> readMemory;
		try {
			readMemory = memoryManager.allocatePages(owner, numMinPagesTotal);
		} catch (MemoryAllocationException e) {
			LOG.error("Can't allocate {} pages from fixed memory pool.", numMinPagesTotal, e);
			throw new RuntimeException(e);
		}
		this.fixedReadMemoryNum = readMemory.size();
		this.writeMemory = new ArrayList<>(numWriteBuffers);
		for (int i = 0; i < numWriteBuffers; i++) {
			this.writeMemory.add(readMemory.remove(readMemory.size() - 1));
		}

		// circular circularQueues pass buffers between the threads
		final CircularQueues circularQueues = new CircularQueues();

		// allocate the sort buffers and fill empty queue with them
		final Iterator<MemorySegment> segments = readMemory.iterator();
		final int perRequestBuffersNum = (int) (perRequestMemorySize / memoryManager.getPageSize());
		final int additionalLimitNumPages =
				(int) ((preferredMemorySize - reservedMemorySize) / memoryManager.getPageSize());
		final int eachBufferAdditionalLimitNumPages = (int) (additionalLimitNumPages / numSortBuffers);

		LOG.info("BinaryExternalSorter with initial memory segments {},And the preferred memory {} segments, " +
				"per request {} segments from floating memory pool.", numMinPagesTotal, (int)
				(preferredMemorySize / memoryManager.getPageSize()), perRequestBuffersNum);

		this.sortBuffers = new ArrayList<>();
		for (int i = 0; i < numSortBuffers; i++) {
			// grab some memory
			final List<MemorySegment> sortSegments = new ArrayList<>(numSegmentsPerSortBuffer);
			for (int k = (i == numSortBuffers - 1 ? Integer.MAX_VALUE : numSegmentsPerSortBuffer); k > 0 && segments
					.hasNext(); k--) {
				sortSegments.add(segments.next());
			}
			this.sortReadMemory.add(sortSegments);
			final BinaryInMemorySortBuffer buffer = BinaryInMemorySortBuffer.createBuffer(memoryManager,
					normalizedKeyComputer, inputSerializer, serializer, comparator, sortSegments,
					eachBufferAdditionalLimitNumPages, perRequestBuffersNum);

			// add to empty queue
			CircularElement element = new CircularElement(i, buffer, sortSegments);
			circularQueues.empty.add(element);
			this.sortBuffers.add(buffer);
		}

		// exception handling
		ExceptionHandler<IOException> exceptionHandler = exception -> {
			// forward exception
			if (!closed) {
				setResultIteratorException(exception);
				close();
			}
		};

		// init adding currWriteBuffer
		this.exceptionHandler = exceptionHandler;
		this.circularQueues = circularQueues;

		bytesUntilSpilling = ((long) (startSpillingFraction * sortMemory));

		// check if we should directly spill
		if (bytesUntilSpilling < 1) {
			bytesUntilSpilling = 0;
			// add the spilling marker
			this.circularQueues.sort.add(SPILLING_MARKER);
		}

		this.channelManager = new SpillChannelManager();
		this.merger = new BinaryExternalMerger(
				ioManager, memoryManager.getPageSize(),
				maxNumFileHandles, channelManager,
				(BinaryRowSerializer) serializer.duplicate(), comparator);

		// start the thread that sorts the buffers
		this.sortThread = getSortingThread(exceptionHandler, circularQueues);

		// start the thread that handles spilling to secondary storage
		this.spillThread = getSpillingThread(
				exceptionHandler, circularQueues, memoryManager, ioManager,
				(BinaryRowSerializer) serializer.duplicate(), comparator,
				this.sortReadMemory, this.writeMemory, maxNumFileHandles, merger);

		// propagate the context class loader to the spawned threads
		ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
		if (contextLoader != null) {
			if (this.sortThread != null) {
				this.sortThread.setContextClassLoader(contextLoader);
			}
			if (this.spillThread != null) {
				this.spillThread.setContextClassLoader(contextLoader);
			}
		}
	}

	// ------------------------------------------------------------------------
	//                           Factory Methods
	// ------------------------------------------------------------------------

	/**
	 * Starts all the threads that are used by this sorter.
	 */
	public void startThreads() {
		if (this.sortThread != null) {
			this.sortThread.start();
		}
		if (this.spillThread != null) {
			this.spillThread.start();
		}
	}

	/**
	 * Shuts down all the threads initiated by this sorter. Also releases all previously allocated
	 * memory, if it has not yet been released by the threads, and closes and deletes all channels
	 * (removing the temporary files).
	 *
	 * <p>The threads are set to exit directly, but depending on their operation, it may take a
	 * while to actually happen. The sorting thread will for example not finish before the current
	 * batch is sorted. This method attempts to wait for the working thread to exit. If it is
	 * however interrupted, the method exits immediately and is not guaranteed how long the threads
	 * continue to exist and occupy resources afterwards.
	 */
	@Override
	public void close() {
		// check if the sorter has been closed before
		synchronized (this) {
			if (this.closed) {
				return;
			}

			// mark as closed
			this.closed = true;
		}

		// from here on, the code is in a try block, because even through errors might be thrown in this block,
		// we need to make sure that all the memory is released.
		try {
			// if the result iterator has not been obtained yet, set the exception
			synchronized (this.iteratorLock) {
				if (this.iteratorException == null) {
					this.iteratorException = new IOException("The sorter has been closed.");
					this.iteratorLock.notifyAll();
				}
			}

			// stop all the threads
			if (this.sortThread != null) {
				try {
					this.sortThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down sorter thread: " + t.getMessage(), t);
				}
			}
			if (this.spillThread != null) {
				try {
					this.spillThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down spilling thread: " + t.getMessage(), t);
				}
			}

			try {
				if (this.sortThread != null) {
					this.sortThread.join();
					this.sortThread = null;
				}

				if (this.spillThread != null) {
					this.spillThread.join();
					this.spillThread = null;
				}
			} catch (InterruptedException iex) {
				LOG.debug("Closing of sort/merger was interrupted. " +
						"The reading/sorting/spilling threads may still be working.", iex);
			}
		} finally {

			// RELEASE ALL MEMORY. If the threads and channels are still running, this should cause
			// exceptions, because their memory segments are freed
			try {
				if (!this.writeMemory.isEmpty()) {
					this.memoryManager.release(this.writeMemory);
				}
				this.writeMemory.clear();
			} catch (Throwable ignored) {
				LOG.info("error.", ignored);
			}

			try {
				this.sortBuffers.stream().forEach(sortBuffer -> sortBuffer.returnToSegmentPool());
				this.sortBuffers.clear();
			} catch (Throwable ignored) {
				LOG.info("error.", ignored);
			}

			List<MemorySegment> mergeReadMemory = new ArrayList<>();
			for (List<MemorySegment> segs : sortReadMemory) {
				mergeReadMemory.addAll(segs);
			}

			try {
				if (mergeReadMemory.size() > fixedReadMemoryNum) {
					int releaseNum = mergeReadMemory.size() - fixedReadMemoryNum;
					MemUtil.releaseSpecificNumFloatingSegments(memoryManager, mergeReadMemory, releaseNum);
				}
			} catch (Throwable ignored) {
				LOG.info("error.", ignored);
			}

			try {
				if (!mergeReadMemory.isEmpty()) {
					this.memoryManager.release(mergeReadMemory);
				}
				mergeReadMemory.clear();
			} catch (Throwable ignored) {
				LOG.info("error.", ignored);
			}
			sortReadMemory.clear();

			// Eliminate object references for MemorySegments.
			circularQueues = null;
			currWriteBuffer = null;
			iterator = null;

			merger.close();
			channelManager.close();
		}
	}

	protected ThreadBase getSortingThread(ExceptionHandler<IOException> exceptionHandler,
			CircularQueues queues) {
		return new SortingThread(exceptionHandler, queues);
	}

	protected SpillingThread getSpillingThread(ExceptionHandler<IOException> exceptionHandler,
			CircularQueues queues, MemoryManager memoryManager, IOManager ioManager,
			BinaryRowSerializer serializer, RecordComparator comparator,
			List<List<MemorySegment>> sortReadMemorys, List<MemorySegment> writeMemory,
			int maxFileHandles, BinaryExternalMerger merger) {
		return new SpillingThread(exceptionHandler, queues,
				memoryManager, ioManager, serializer, comparator, sortReadMemorys, writeMemory, maxFileHandles, merger);
	}

	public void write(BaseRow current) throws IOException {
		checkArgument(!writingDone, "Adding already done!");
		try {
			while (true) {
				if (closed) {
					throw new IOException("Already closed!", iteratorException);
				}

				synchronized (writeLock) {
					// grab the next buffer
					if (currWriteBuffer == null) {
						try {
							currWriteBuffer = this.circularQueues.empty.poll(1, TimeUnit.SECONDS);
							if (currWriteBuffer == null) {
								// maybe something happened, release lock.
								continue;
							}
							if (!currWriteBuffer.buffer.isEmpty()) {
								throw new IOException("New buffer is not empty.");
							}
						} catch (InterruptedException iex) {
							throw new IOException(iex);
						}
					}

					final BinaryInMemorySortBuffer buffer = currWriteBuffer.buffer;

					if (LOG.isDebugEnabled()) {
						LOG.debug("Retrieved empty read buffer " + currWriteBuffer.id + ".");
					}

					long occupancy = buffer.getOccupancy();
					if (!buffer.write(current)) {
						if (buffer.isEmpty()) {
							// did not fit in a fresh buffer, must be large...
							throw new IOException("The record exceeds the maximum size of a sort buffer (current maximum: "
									+ buffer.getCapacity() + " bytes).");
						} else {
							// buffer is full, send the buffer
							if (LOG.isDebugEnabled()) {
								LOG.debug("Emitting full buffer: " + currWriteBuffer.id + ".");
							}

							this.circularQueues.sort.add(currWriteBuffer);

							// Deadlocks may occur when there are fewer MemorySegments, because of
							// the fragmentation of buffer.getOccupancy ().
							if (bytesUntilSpilling > 0 && circularQueues.empty.size() == 0) {
								bytesUntilSpilling = 0;
								this.circularQueues.sort.add(SPILLING_MARKER);
							}

							currWriteBuffer = null;
							// continue to process current record.
						}
					} else {
						// successfully added record
						// it may be that the last currWriteBuffer would have crossed the
						// spilling threshold, so check it
						if (bytesUntilSpilling > 0) {
							bytesUntilSpilling -= buffer.getOccupancy() - occupancy;
							if (bytesUntilSpilling <= 0) {
								bytesUntilSpilling = 0;
								this.circularQueues.sort.add(SPILLING_MARKER);
							}
						}
						break;
					}
				}
			}
		} catch (Throwable e) {
			IOException ioe = new IOException(e);
			if (this.exceptionHandler != null) {
				this.exceptionHandler.handleException(ioe);
			}
			throw ioe;
		}
	}

	@VisibleForTesting
	public void write(MutableObjectIterator<BinaryRow> iterator) throws IOException {
		BinaryRow row = serializer.createInstance();
		while ((row = iterator.next(row)) != null) {
			write(row);
		}
	}

	@Override
	public List<SortedDataFile<BinaryRow>> getRemainingSortedDataFiles() throws InterruptedException {
		return null;
	}

	@Override
	public MutableObjectIterator<BinaryRow> getIterator() throws InterruptedException {
		if (!writingDone) {
			writingDone = true;

			if (currWriteBuffer != null) {
				this.circularQueues.sort.add(currWriteBuffer);
			}

			// add the sentinel to notify the receivers that the work is done
			// send the EOF marker
			this.circularQueues.sort.add(EOF_MARKER);
			LOG.debug("Sending done.");
		}

		synchronized (this.iteratorLock) {
			// wait while both the iterator and the exception are not set
			while (this.iterator == null && this.iteratorException == null) {
				this.iteratorLock.wait();
			}

			if (this.iteratorException != null) {
				throw new RuntimeException("Error obtaining the sorted input: " + this.iteratorException.getMessage(),
						this.iteratorException);
			} else {
				return this.iterator;
			}
		}
	}

	// ------------------------------------------------------------------------
	// Inter-Thread Communication
	// ------------------------------------------------------------------------

	/**
	 * Sets the result iterator. By setting the result iterator, all threads that are waiting for
	 * the result
	 * iterator are notified and will obtain it.
	 *
	 * @param iterator The result iterator to set.
	 */
	protected final void setResultIterator(MutableObjectIterator<BinaryRow> iterator) {
		synchronized (this.iteratorLock) {
			// set the result iterator only, if no exception has occurred
			if (this.iteratorException == null) {
				this.iterator = iterator;
				this.iteratorLock.notifyAll();
			}
		}
	}

	/**
	 * Reports an exception to all threads that are waiting for the result iterator.
	 *
	 * @param ioex The exception to be reported to the threads that wait for the result iterator.
	 */
	protected final void setResultIteratorException(IOException ioex) {
		synchronized (this.iteratorLock) {
			if (this.iteratorException == null) {
				this.iteratorException = ioex;
				this.iteratorLock.notifyAll();
			}
		}
	}

	/**
	 * Class representing buffers that circulate between the reading, sorting and spilling thread.
	 */
	protected static final class CircularElement {

		final int id; // just for debug.
		final BinaryInMemorySortBuffer buffer;
		final List<MemorySegment> memory; // for release memory

		public CircularElement() {
			this.id = -1;
			this.buffer = null;
			this.memory = null;
		}

		public CircularElement(int id, BinaryInMemorySortBuffer buffer, List<MemorySegment> memory) {
			this.id = id;
			this.buffer = buffer;
			this.memory = memory;
		}
	}

	/**
	 * Collection of circularQueues that are used for the communication between the threads.
	 */
	protected static final class CircularQueues {

		final BlockingQueue<CircularElement> empty;

		final BlockingQueue<CircularElement> sort;

		final BlockingQueue<CircularElement> spill;

		protected CircularQueues() {
			this.empty = new LinkedBlockingQueue<>();
			this.sort = new LinkedBlockingQueue<>();
			this.spill = new LinkedBlockingQueue<>();
		}
	}

	// ------------------------------------------------------------------------
	// Threads
	// ------------------------------------------------------------------------

	/**
	 * Base class for all working threads in this sorter. The specific threads for sorting,
	 * spilling, etc... extend this subclass.
	 *
	 * <p>The threads are designed to terminate themselves when the task they are set up to do is
	 * completed. Further more, they terminate immediately when the <code>shutdown()</code> method
	 * is called.
	 */
	protected abstract static class ThreadBase extends Thread implements Thread.UncaughtExceptionHandler {

		/**
		 * The queue of empty buffer that can be used for reading.
		 */
		protected final CircularQueues queues;

		/**
		 * The exception handler for any problems.
		 */
		private final ExceptionHandler<IOException> exceptionHandler;

		/**
		 * The flag marking this thread as alive.
		 */
		private volatile boolean alive;

		/**
		 * Creates a new thread.
		 *
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param name             The name of the thread.
		 * @param queues           The circularQueues used to pass buffers between the threads.
		 */
		protected ThreadBase(ExceptionHandler<IOException> exceptionHandler, String name,
				CircularQueues queues) {
			// thread setup
			super(name);
			this.setDaemon(true);

			// exception handling
			this.exceptionHandler = exceptionHandler;
			this.setUncaughtExceptionHandler(this);

			this.queues = queues;
			this.alive = true;
		}

		/**
		 * Implements exception handling and delegates to go().
		 */
		public void run() {
			try {
				go();
			} catch (Throwable t) {
				internalHandleException(new IOException("Thread '" + getName() + "' terminated due to an exception: "
						+ t.getMessage(), t));
			}
		}

		/**
		 * Equivalent to the run() method.
		 *
		 * @throws IOException Exceptions that prohibit correct completion of the work may be thrown
		 *                     by the thread.
		 */
		protected abstract void go() throws IOException;

		/**
		 * Checks whether this thread is still alive.
		 *
		 * @return true, if the thread is alive, false otherwise.
		 */
		public boolean isRunning() {
			return this.alive;
		}

		/**
		 * Forces an immediate shutdown of the thread. Looses any state and all buffers that the
		 * thread is currently
		 * working on. This terminates cleanly for the JVM, but looses intermediate results.
		 */
		public void shutdown() {
			this.alive = false;
			this.interrupt();
		}

		/**
		 * Internally handles an exception and makes sure that this method returns without a
		 * problem.
		 *
		 * @param ioex The exception to handle.
		 */
		protected final void internalHandleException(IOException ioex) {
			if (!isRunning()) {
				// discard any exception that occurs when after the thread is killed.
				return;
			}
			if (this.exceptionHandler != null) {
				try {
					this.exceptionHandler.handleException(ioex);
				} catch (Throwable ignored) {
				}
			}
		}

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			internalHandleException(new IOException("Thread '" + t.getName()
					+ "' terminated due to an uncaught exception: " + e.getMessage(), e));
		}
	}

	/**
	 * The thread that sorts filled buffers.
	 */
	protected static class SortingThread extends ThreadBase {

		private final IndexedSorter sorter;

		/**
		 * Creates a new sorting thread.
		 *
		 * @param exceptionHandler The exception handler to call for all exceptions.
		 * @param queues           The circularQueues used to pass buffers between the threads.
		 */
		public SortingThread(ExceptionHandler<IOException> exceptionHandler,
				CircularQueues queues) {
			super(exceptionHandler, "SortMerger sorting thread", queues);

			// members
			this.sorter = new QuickSort();
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException {
			boolean alive = true;

			// loop as long as the thread is marked alive
			while (isRunning() && alive) {
				CircularElement element;
				try {
					element = this.queues.sort.take();
				} catch (InterruptedException iex) {
					if (isRunning()) {
						if (LOG.isErrorEnabled()) {
							LOG.error(
									"Sorting thread was interrupted (without being shut down) while grabbing a buffer. " +
											"Retrying to grab buffer...");
						}
						continue;
					} else {
						return;
					}
				}

				if (element != EOF_MARKER && element != SPILLING_MARKER) {

					if (element.buffer.size() == 0) {
						element.buffer.reset();
						this.queues.empty.add(element);
						continue;
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("Sorting buffer " + element.id + ".");
					}

					this.sorter.sort(element.buffer);

					if (LOG.isDebugEnabled()) {
						LOG.debug("Sorted buffer " + element.id + ".");
					}
				} else if (element == EOF_MARKER) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Sorting thread done.");
					}
					alive = false;
				}
				this.queues.spill.add(element);
			}
		}
	}

	/**
	 * The thread that handles the spilling of intermediate results and sets up the merging. It also
	 * merges the
	 * channels until sufficiently few channels remain to perform the final streamed merge.
	 */
	protected class SpillingThread extends ThreadBase {

		protected final MemoryManager memManager;            // memory manager to release memory

		protected final IOManager ioManager;                // I/O manager to create channels

		protected final BinaryRowSerializer serializer;        // The serializer for the data type

		protected final List<MemorySegment> writeMemory;    // memory segments for writing

		protected final RecordComparator comparator;

		protected final List<List<MemorySegment>> mergeReadMemorys;    // memory segments for sorting/reading

		protected final int maxFanIn;

		protected final int numWriteBuffersToCluster;

		private final BinaryExternalMerger merger;

		/**
		 * Creates the spilling thread.
		 *  @param exceptionHandler  The exception handler to call for all exceptions.
		 * @param queues            The circularQueues used to pass buffers between the threads.
		 * @param memManager        The memory manager used to allocate buffers for the readers and
 *                          writers.
		 * @param ioManager         The I/I manager used to instantiate readers and writers from.
		 * @param serializer
		 * @param comparator
		 * @param sortReadMemorys
		 * @param writeMemory
		 * @param maxNumFileHandles
		 * @param merger
		 */
		public SpillingThread(ExceptionHandler<IOException> exceptionHandler,
				CircularQueues queues, MemoryManager memManager, IOManager ioManager,
				BinaryRowSerializer serializer, RecordComparator comparator,
				List<List<MemorySegment>> sortReadMemorys, List<MemorySegment> writeMemory,
				int maxNumFileHandles, BinaryExternalMerger merger) {
			super(exceptionHandler, "SortMerger spilling thread", queues);
			this.memManager = memManager;
			this.ioManager = ioManager;
			this.serializer = serializer;
			this.comparator = comparator;
			this.mergeReadMemorys = sortReadMemorys;
			this.writeMemory = writeMemory;
			this.maxFanIn = maxNumFileHandles;
			this.numWriteBuffersToCluster = writeMemory.size() >= 4 ? writeMemory.size() / 2 : 1;
			this.merger = merger;
		}

		/**
		 * Entry point of the thread.
		 */
		public void go() throws IOException {

			final Queue<CircularElement> cache = new ArrayDeque<>();
			CircularElement element;
			boolean cacheOnly = false;

			// ------------------- In-Memory Cache ------------------------
			// fill cache
			while (isRunning()) {
				// take next currWriteBuffer from queue
				try {
					element = this.queues.spill.take();
				} catch (InterruptedException iex) {
					throw new IOException("The spilling thread was interrupted.");
				}

				if (element == SPILLING_MARKER) {
					break;
				} else if (element == EOF_MARKER) {
					cacheOnly = true;
					break;
				}
				cache.add(element);
			}

			// check whether the thread was canceled
			if (!isRunning()) {
				return;
			}

			// ------------------- In-Memory Merge ------------------------
			if (cacheOnly) {

				List<MutableObjectIterator<BinaryRow>> iterators = new ArrayList<>(cache.size());

				for (CircularElement cached : cache) {
					iterators.add(cached.buffer.getIterator());
				}

				disposeSortBuffers(true);

				// set lazy iterator
				List<BinaryRow> reusableEntries = new ArrayList<>();
				for (int i = 0; i < iterators.size(); i++) {
					reusableEntries.add(serializer.createInstance());
				}
				setResultIterator(iterators.isEmpty() ? EmptyMutableObjectIterator.get() :
						iterators.size() == 1 ? iterators.get(0) : new BinaryMergeIterator<>(
								iterators, reusableEntries, comparator::compare));
				return;
			}

			// ------------------- Spilling Phase ------------------------

			final FileIOChannel.Enumerator enumerator =
					this.ioManager.createChannelEnumerator();
			List<ChannelWithMeta> channelIDs = new ArrayList<>();

			// loop as long as the thread is marked alive and we do not see the final currWriteBuffer
			while (isRunning()) {
				try {
					// TODO let cache in memory instead of disk.
					element = cache.isEmpty() ? queues.spill.take() : cache.poll();
				} catch (InterruptedException iex) {
					if (isRunning()) {
						LOG.error("Sorting thread was interrupted (without being shut down) while grabbing a buffer. " +
								"Retrying to grab buffer...");
						continue;
					} else {
						return;
					}
				}

				// check if we are still running
				if (!isRunning()) {
					return;
				}
				// check if this is the end-of-work buffer
				if (element == EOF_MARKER) {
					break;
				}

				if (element.buffer.getOccupancy() > 0) {
					// open next channel
					FileIOChannel.ID channel = enumerator.next();
					channelManager.addChannel(channel);

					// create writer
					BlockChannelWriter<MemorySegment> writer = null;
					int bytesInLastBuffer;
					int blockCount;

					try {
						writer = this.ioManager.createBlockChannelWriter(channel);
						HeaderlessChannelWriterOutputView output = new HeaderlessChannelWriterOutputView(
								writer, this.writeMemory, this.memManager.getPageSize());
						element.buffer.writeToOutput(output);
						numSpillFiles++;
						spillInBytes += writer.getSize();
						LOG.info("here spill the {}th sort buffer data with {} bytes", numSpillFiles, spillInBytes);
						bytesInLastBuffer = output.close();
						blockCount = output.getBlockCount();
					} catch (IOException e) {
						if (writer != null) {
							writer.closeAndDelete();
						}
						throw e;
					}

					channelIDs.add(new ChannelWithMeta(channel, blockCount, bytesInLastBuffer));
				}

				// pass empty sort-buffer to reading thread
				element.buffer.reset();
				this.queues.empty.add(element);
			}

			// clear the sort buffers, but do not return the memory to the manager, as we use it for merging
			disposeSortBuffers(false);

			// ------------------- Merging Phase ------------------------

			// make sure we have enough memory to merge and for large record handling
			List<MemorySegment> mergeReadMemory = new ArrayList<>();
			for (List<MemorySegment> segs : mergeReadMemorys) {
				mergeReadMemory.addAll(segs);
			}

			// merge channels until sufficient file handles are available
			while (isRunning() && channelIDs.size() > this.maxFanIn) {
				channelIDs = merger.mergeChannelList(channelIDs, mergeReadMemory, this.writeMemory);
			}

			// from here on, we won't write again
			this.memManager.release(this.writeMemory);
			this.writeMemory.clear();

			// check if we have spilled some data at all
			if (channelIDs.isEmpty()) {
				setResultIterator(EmptyMutableObjectIterator.get());
			} else {
				// Beginning final merge.

				// allocate the memory for the final merging step
				List<List<MemorySegment>> readBuffers = new ArrayList<>(channelIDs.size());

				// allocate the read memory and register it to be released
				getSegmentsForReaders(readBuffers, mergeReadMemory, channelIDs.size());

				List<FileIOChannel> openChannels = new ArrayList<>();
				BinaryMergeIterator iterator = merger.getMergingIterator(
						channelIDs, readBuffers, openChannels);
				channelManager.addOpenChannels(openChannels);

				setResultIterator(iterator);
			}

			// Spilling and merging thread done.
		}

		/**
		 * Releases the memory that is registered for in-memory sorted run generation.
		 */
		protected final void disposeSortBuffers(boolean releaseMemory) {
			while (!this.queues.empty.isEmpty()) {
				try {
					CircularElement element = this.queues.empty.take();
					element.buffer.dispose();
					if (releaseMemory) {
						this.memManager.release(element.memory);
					}
				} catch (InterruptedException iex) {
					if (isRunning()) {
						LOG.error("Spilling thread was interrupted (without being shut down) while collecting empty " +
								"buffers to release them. Retrying to collect buffers...");
					} else {
						return;
					}
				}
			}
		}
	}

	public long getUsedMemoryInBytes() {
		long usedSizeInBytes = 0;
		for (BinaryInMemorySortBuffer sortBuffer : sortBuffers) {
			usedSizeInBytes += sortBuffer.getOccupancy();
		}
		return usedSizeInBytes;
	}

	public long getNumSpillFiles() {
		return numSpillFiles;
	}

	public long getSpillInBytes() {
		return spillInBytes;
	}
}
