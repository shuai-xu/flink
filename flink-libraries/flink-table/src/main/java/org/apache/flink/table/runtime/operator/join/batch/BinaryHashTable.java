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

package org.apache.flink.table.runtime.operator.join.batch;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BulkBlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.util.BitSet;
import org.apache.flink.table.codegen.JoinConditionFunction;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.table.util.BinaryRowUtil;
import org.apache.flink.table.util.MemUtil;
import org.apache.flink.table.util.MemorySegmentPool;
import org.apache.flink.table.util.PagedChannelReaderInputViewIterator;
import org.apache.flink.table.util.RowIterator;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An implementation of a Hybrid Hash Join. The join starts operating in memory and gradually
 * starts spilling contents to disk, when the memory is not sufficient. It does not need to know a
 * priority how large the input will be.
 *
 * <p>The design of this class follows in many parts the design presented in
 * "Hash joins and hash teams in Microsoft SQL Server", by Goetz Graefe et al. In its current state,
 * the implementation lacks features like dynamic role reversal, partition tuning, or histogram
 * guided partitioning.</p>
 */
public class BinaryHashTable implements MemorySegmentPool {

	private static final Logger LOG = LoggerFactory.getLogger(BinaryHashTable.class);

	/**
	 * The maximum number of partitions, which defines the spilling granularity. Each recursion,
	 * the data is divided maximally into that many partitions, which are processed in one chuck.
	 */
	private static final int MAX_NUM_PARTITIONS = Byte.MAX_VALUE;

	/**
	 * The maximum number of recursive partitionings that the join does before giving up.
	 */
	private static final int MAX_RECURSION_DEPTH = 3;

	/**
	 * The minimum number of memory segments the hash join needs to be supplied with in order to
	 * work.
	 */
	private static final int MIN_NUM_MEMORY_SEGMENTS = 33;

	/**
	 * The I/O manager used to instantiate writers for the spilled partitions.
	 */
	private final IOManager ioManager;

	/**
	 * The utilities to serialize the build side data types.
	 */
	final BinaryRowSerializer binaryBuildSideSerializer;

	private final AbstractRowSerializer originBuildSideSerializer;

	/**
	 * The utilities to serialize the probe side data types.
	 */
	private final BinaryRowSerializer binaryProbeSideSerializer;

	private final AbstractRowSerializer originProbeSideSerializer;

	/**
	 * The utilities to hash and compare the build side data types.
	 */
	private final Projection<BaseRow, BinaryRow> buildSideProjection;

	/**
	 * The utilities to hash and compare the probe side data types.
	 */
	private final Projection<BaseRow, BinaryRow> probeSideProjection;

	/**
	 * The free memory segments currently available to the hash join.
	 */
	final List<MemorySegment> availableMemory;

	/**
	 * The queue of buffers that can be used for write-behind. Any buffer that is written
	 * asynchronously to disk is returned through this queue. hence, it may sometimes contain more
	 */
	private final LinkedBlockingQueue<MemorySegment> writeBehindBuffers;

	/**
	 * The size of the segments used by the hash join buckets. All segments must be of equal size to
	 * ease offset computations.
	 */
	private final int segmentSize;

	/**
	 * The number of write-behind buffers used.
	 */
	private final int numWriteBehindBuffers = 6;

	final int bucketsPerSegment;

	/**
	 * The number of hash table buckets in a single memory segment - 1.
	 * Because memory segments can be comparatively large, we fit multiple buckets into one memory
	 * segment.
	 * This variable is a mask that is 1 in the lower bits that define the number of a bucket
	 * in a segment.
	 */
	final int bucketsPerSegmentMask;

	/**
	 * The number of bits that describe the position of a bucket in a memory segment. Computed as
	 * log2(bucketsPerSegment).
	 */
	final int bucketsPerSegmentBits;

	/** Flag to enable/disable bloom filters for spilled partitions. */
	final boolean useBloomFilters;

	/**
	 * The partitions that are built by processing the current partition.
	 */
	final ArrayList<BinaryHashPartition> partitionsBeingBuilt;

	/**
	 * BitSet which used to mark whether the element(int build side) has successfully matched during
	 * probe phase. As there are 9 elements in each bucket, we assign 2 bytes to BitSet.
	 */
	final BitSet probedSet = new BitSet(2);

	/**
	 * The partitions that have been spilled previously and are pending to be processed.
	 */
	private final ArrayList<BinaryHashPartition> partitionsPending;

	private final JoinConditionFunction condFunc;

	private final boolean reverseJoin;

	/**
	 * Should filter null keys.
	 */
	private final int[] nullFilterKeys;

	/**
	 * No keys need to filter null.
	 */
	private final boolean nullSafe;

	/**
	 * Filter null to all keys.
	 */
	private final boolean filterAllNulls;

	/**
	 * Iterator over the elements in the hash table.
	 */
	LookupBucketIterator bucketIterator;
	/**
	 * Iterator over the elements from the probe side.
	 */
	private ProbeIterator probeIterator;
	/**
	 * The channel enumerator that is used while processing the current partition to create
	 * channels for the spill partitions it requires.
	 */
	private FileIOChannel.Enumerator currentEnumerator;

	/**
	 * The number of buffers in the write behind queue that are actually not write behind buffers,
	 * but regular buffers that only have not yet returned. This is part of an optimization that the
	 * spilling code needs not wait until the partition is completely spilled before proceeding.
	 */
	private int writeBehindBuffersAvailable;
	/**
	 * The recursion depth of the partition that is currently processed. The initial table
	 * has a recursion depth of 0. Partitions spilled from a table that is built for a partition
	 * with recursion depth <i>n</i> have a recursion depth of <i>n+1</i>.
	 */
	private int currentRecursionDepth;
	/**
	 * Flag indicating that the closing logic has been invoked.
	 */
	private AtomicBoolean closed = new AtomicBoolean();

	final HashJoinType type;

	/**
	 * The reader for the spilled-file of the build partition that is currently read.
	 */
	private BlockChannelReader<MemorySegment> currentSpilledBuildSide;
	/**
	 * The reader for the spilled-file of the probe partition that is currently read.
	 */
	private BlockChannelReader<MemorySegment> currentSpilledProbeSide;

	private RowIterator<BinaryRow> buildIterator;

	private boolean probeMatchedPhase = true;

	private boolean buildIterVisited = false;

	/**
	 * Try to make the buildSide rows distinct.
	 */
	boolean tryDistinctBuildRow = false;

	private BinaryRow probeKey;
	private BaseRow probeRow;

	BinaryRow reuseBuildRow;

	final int segmentSizeBits;

	final int segmentSizeMask;
	private final int avgRecordLen;
	private final long buildRowCount;

	/**
	 * The total reserved number of memory segments available to the hash join.
	 */
	private final int reservedNumBuffers;
	/**
	 * The total preferred number of memory segments available to the hash join.
	 */
	private final int preferedNumBuffers;

	/**
	 * record number of the allocated segments from the floating pool.
	 */
	private int allocatedFloatingNum;
	private final int perRequestNumBuffers;
	private final MemoryManager memManager;
	/**
	 * The owner to associate with the memory segment.
	 */
	private final Object owner;

	private transient long numSpillFiles;
	private transient long spillInBytes;

	public BinaryHashTable(
			Object owner,
			AbstractRowSerializer buildSideSerializer,
			AbstractRowSerializer probeSideSerializer,
			Projection<BaseRow, BinaryRow> buildSideProjection,
			Projection<BaseRow, BinaryRow> probeSideProjection,
			MemoryManager memManager,
			long reservedMemorySize,
			IOManager ioManager, int avgRecordLen, int buildRowCount,
			boolean useBloomFilters, HashJoinType type,
			JoinConditionFunction condFunc, boolean reverseJoin, boolean[] filterNulls,
			boolean tryDistinctBuildRow) {
		this(owner, buildSideSerializer, probeSideSerializer, buildSideProjection, probeSideProjection, memManager,
				reservedMemorySize, reservedMemorySize, 0, ioManager, avgRecordLen, buildRowCount, useBloomFilters, type, condFunc,
				reverseJoin, filterNulls, tryDistinctBuildRow);
	}

	public BinaryHashTable(
			Object owner,
			AbstractRowSerializer buildSideSerializer,
			AbstractRowSerializer probeSideSerializer,
			Projection<BaseRow, BinaryRow> buildSideProjection,
			Projection<BaseRow, BinaryRow> probeSideProjection,
			MemoryManager memManager,
			long reservedMemorySize,
			long preferredMemorySize,
			long perRequestMemorySize,
			IOManager ioManager, int avgRecordLen, long buildRowCount,
			boolean useBloomFilters, HashJoinType type,
			JoinConditionFunction condFunc, boolean reverseJoin, boolean[] filterNulls,
			boolean tryDistinctBuildRow) {
		this.avgRecordLen = avgRecordLen;
		this.buildRowCount = buildRowCount;
		// some sanity checks first
		this.owner = owner;
		this.reservedNumBuffers = (int) (reservedMemorySize / memManager.getPageSize());
		// some sanity checks first
		checkArgument(reservedNumBuffers >= MIN_NUM_MEMORY_SEGMENTS);
		this.preferedNumBuffers = (int) (preferredMemorySize / memManager.getPageSize());
		this.perRequestNumBuffers = (int) (perRequestMemorySize / memManager.getPageSize());
		this.availableMemory = new ArrayList<>(this.reservedNumBuffers);
		try {
			List<MemorySegment> allocates = memManager.allocatePages(owner, this.reservedNumBuffers);
			this.availableMemory.addAll(allocates);
			allocates.clear();
		} catch (MemoryAllocationException e) {
			LOG.error("Out of memory", e);
			throw new RuntimeException(e);
		}
		this.memManager = memManager;
		// assign the members
		this.originBuildSideSerializer = buildSideSerializer;
		this.binaryBuildSideSerializer = new BinaryRowSerializer(
				buildSideSerializer.getTypes());
		this.reuseBuildRow = binaryBuildSideSerializer.createInstance();
		this.originProbeSideSerializer = probeSideSerializer;
		this.binaryProbeSideSerializer = new BinaryRowSerializer(
				originProbeSideSerializer.getTypes());

		this.buildSideProjection = buildSideProjection;
		this.probeSideProjection = probeSideProjection;
		this.ioManager = ioManager;
		this.useBloomFilters = useBloomFilters;
		this.type = type;
		this.condFunc = condFunc;
		this.reverseJoin = reverseJoin;
		this.nullFilterKeys = NullAwareJoinHelper.getNullFilterKeys(filterNulls);
		this.nullSafe = nullFilterKeys.length == 0;
		this.filterAllNulls = nullFilterKeys.length == filterNulls.length;
		this.tryDistinctBuildRow = !type.buildLeftSemiOrAnti() && tryDistinctBuildRow;

		// check the size of the first buffer and record it. all further buffers must have the
		// same size. the size must also be a power of 2
		this.segmentSize = memManager.getPageSize();
		checkArgument((this.segmentSize & this.segmentSize - 1) == 0,
				"Hash Table requires buffers whose size is a power of 2.");
		this.bucketsPerSegment = this.segmentSize >> BinaryHashBucketArea.BUCKET_SIZE_BITS;
		checkArgument(bucketsPerSegment != 0,
				"Hash Table requires buffers of at least " + BinaryHashBucketArea.BUCKET_SIZE + " bytes.");
		this.bucketsPerSegmentMask = bucketsPerSegment - 1;
		this.bucketsPerSegmentBits = MathUtils.log2strict(bucketsPerSegment);

		// take away the write behind buffers
		this.writeBehindBuffers = new LinkedBlockingQueue<>();
		this.partitionsBeingBuilt = new ArrayList<>();
		this.partitionsPending = new ArrayList<>();

		this.segmentSizeBits = MathUtils.log2strict(segmentSize);
		this.segmentSizeMask = segmentSize - 1;

		// because we allow to open and close multiple times, the state is initially closed
		this.closed.set(true);

		LOG.info(String.format("Initialize hash table with %d memory segments, each size [%d], the reserved memory %d" +
				" MB, the preferred memory %d MB, per allocate {} segments from floating memory pool.",
				reservedNumBuffers, segmentSize, (long) reservedNumBuffers * segmentSize / 1024 / 1024,
				(long) preferedNumBuffers * segmentSize / 1024 / 1024), perRequestNumBuffers);

		initWrite();
	}

	// ========================== build phase public method ======================================

	/**
	 * Put a build side row to hash table.
	 */
	public void putBuildRow(BaseRow row) throws IOException {
		final int hashCode = hash(this.buildSideProjection.apply(row).hashCode(), 0);
		// TODO: combine key projection and build side conversion to code gen.
		insertIntoTable(originBuildSideSerializer.baseRowToBinary(row), hashCode);
	}

	/**
	 * End build phase.
	 */
	public void endBuild() throws IOException {
		// finalize the partitions
		for (BinaryHashPartition p : this.partitionsBeingBuilt) {
			p.finalizeBuildPhase(this.ioManager, this.currentEnumerator, this.writeBehindBuffers);
		}

		// the first prober is the probe-side input, but the input is null at beginning
		this.probeIterator = new ProbeIterator(this.binaryProbeSideSerializer.createInstance());

		// the bucket iterator can remain constant over the time
		this.bucketIterator = new LookupBucketIterator(this);
	}

	// ========================== probe phase public method ======================================

	/**
	 * Find matched build side rows for a probe row.
	 * @return return false if the target partition has spilled, we will spill this probe row too.
	 *         The row will be re-match in rebuild phase.
	 */
	public boolean tryProbe(BaseRow record) throws IOException {
		if (!this.probeIterator.hasSource()) {
			// set the current probe value when probeIterator is null at the begging.
			this.probeIterator.setInstance(record);
		}
		// calculate the hash
		BinaryRow probeKey = probeSideProjection.apply(record);
		final int hash = hash(probeKey.hashCode(), this.currentRecursionDepth);

		BinaryHashPartition p = this.partitionsBeingBuilt.get(hash % partitionsBeingBuilt.size());

		// for an in-memory partition, process set the return iterators, else spill the probe records
		if (p.isInMemory()) {
			this.probeKey = probeKey;
			this.probeRow = record;
			p.bucketArea.startLookup(hash);
			return true;
		} else {
			if (p.testHashBloomFilter(hash)) {
				BinaryRow row = originProbeSideSerializer.baseRowToBinary(record);
				p.insertIntoProbeBuffer(row);
			}
			return false;
		}
	}

	// ========================== rebuild phase public method ======================================

	/**
	 * Next record from rebuilt spilled partition or build side outer partition.
	 */
	public boolean nextMatching() throws IOException {
		if (type.needSetProbed()) {
			return processProbeIter() || processBuildIter() || prepareNextPartition();
		} else {
			return processProbeIter() || prepareNextPartition();
		}
	}

	public BaseRow getCurrentProbeRow() {
		if (this.probeMatchedPhase) {
			return this.probeIterator.current();
		} else {
			return null;
		}
	}

	public RowIterator<BinaryRow> getBuildSideIterator() {
		return probeMatchedPhase ? bucketIterator : buildIterator;
	}

	// ================================ internal method ===========================================

	/**
	 * Determines the number of buffers to be used for asynchronous write behind. It is currently
	 * computed as the logarithm of the number of buffers to the base 4, rounded up, minus 2.
	 * The upper limit for the number of write behind buffers is however set to six.
	 *
	 * @param numBuffers The number of available buffers.
	 * @return The number
	 */
	@VisibleForTesting
	static int getNumWriteBehindBuffers(int numBuffers) {
		int numIOBufs = (int) (Math.log(numBuffers) / Math.log(4) - 1.5);
		return numIOBufs > 6 ? 6 : numIOBufs;
	}

	/**
	 * Gets the number of partitions to be used for an initial hash-table.
	 */
	private int getPartitioningFanOutNoEstimates() {
		return Math.max(11, findSmallerPrime((int) Math.min(buildRowCount * avgRecordLen / (10 * segmentSize),
				MAX_NUM_PARTITIONS)));
	}

	/**
	 * Let prime number be the numBuckets, to avoid partition hash and bucket hash congruences.
	 */
	private static int findSmallerPrime(int num) {
		for (; num > 1; num--) {
			if (isPrimeNumber(num)) {
				return num;
			}
		}
		return num;
	}

	private static boolean isPrimeNumber(int num){
		if (num == 2) {
			return true;
		}
		if (num < 2 || num % 2 == 0) {
			return false;
		}
		for (int i = 3; i <= Math.sqrt(num); i += 2){
			if (num % i == 0) {
				return false;
			}
		}
		return true;
	}

	/**
	 * The level parameter is needed so that we can have different hash functions when we
	 * recursively apply the partitioning, so that the working set eventually fits into memory.
	 */
	private static int hash(int hashCode, int level) {
		final int rotation = level * 11;
		int code = Integer.rotateLeft(hashCode, rotation);
		return code >= 0 ? code : -(code + 1);
	}

	private void initWrite() {

		// grab the write behind buffers first
		for (int i = this.numWriteBehindBuffers; i > 0; --i) {
			this.writeBehindBuffers.add(getNextBuffer());
		}
		// open builds the initial table by consuming the build-side input
		this.currentRecursionDepth = 0;

		// create the partitions
		final int partitionFanOut = Math.min(getPartitioningFanOutNoEstimates(), maxNumPartition());
		createPartitions(partitionFanOut, 0);

		// sanity checks
		this.closed.set(false);
	}

	private boolean processProbeIter() throws IOException {

		// the prober's source is null at the begging.
		if (this.probeIterator.hasSource()) {
			final ProbeIterator probeIter = this.probeIterator;

			if (!this.probeMatchedPhase) {
				return false;
			}

			BinaryRow next;
			while ((next = probeIter.next()) != null) {
				BinaryRow probeKey = probeSideProjection.apply(next);
				final int hash = hash(probeKey.hashCode(), this.currentRecursionDepth);

				final BinaryHashPartition p = this.partitionsBeingBuilt.get(hash % partitionsBeingBuilt.size());

				// for an in-memory partition, process set the return iterators, else spill the probe records
				if (p.isInMemory()) {
					this.probeKey = probeKey;
					this.probeRow = next;
					p.bucketArea.startLookup(hash);
					return true;
				} else {
					p.insertIntoProbeBuffer(next);
				}
			}
			// -------------- partition done ---------------

			return false;
		} else {
			return false;
		}
	}

	private boolean processBuildIter() throws IOException {
		if (this.buildIterVisited) {
			return false;
		}

		this.probeMatchedPhase = false;
		this.buildIterator = new BuildSideIterator(
				this.binaryBuildSideSerializer, reuseBuildRow,
				this.partitionsBeingBuilt, probedSet, type.equals(HashJoinType.BUILD_LEFT_SEMI));
		this.buildIterVisited = true;
		return true;
	}

	private List<MemorySegment> getTwoBuffer() {
		List<MemorySegment> memory = new ArrayList<>();
		MemorySegment seg1 = getNextBuffer();
		if (seg1 != null) {
			memory.add(seg1);
			MemorySegment seg2 = getNextBuffer();
			if (seg2 != null) {
				memory.add(seg2);
			}
		} else {
			throw new IllegalStateException("Attempting to begin reading spilled partition without any memory available");
		}
		return memory;
	}

	private boolean prepareNextPartition() throws IOException {
		// finalize and cleanup the partitions of the current table
		int buffersAvailable = 0;
		for (final BinaryHashPartition p : this.partitionsBeingBuilt) {
			buffersAvailable += p.finalizeProbePhase(this.availableMemory, this.partitionsPending, type.needSetProbed());
		}

		this.partitionsBeingBuilt.clear();
		this.writeBehindBuffersAvailable += buffersAvailable;

		if (this.currentSpilledBuildSide != null) {
			this.currentSpilledBuildSide.closeAndDelete();
			this.currentSpilledBuildSide = null;
		}

		if (this.currentSpilledProbeSide != null) {
			this.currentSpilledProbeSide.closeAndDelete();
			this.currentSpilledProbeSide = null;
		}

		if (this.partitionsPending.isEmpty()) {
			// no more data
			return false;
		}

		// there are pending partitions
		final BinaryHashPartition p = this.partitionsPending.get(0);
		LOG.info(String.format("Begin to process spilled partition [%d]", p.getPartitionNumber()));

		if (p.probeSideRecordCounter == 0) {
			// unprobed spilled partitions are only re-processed for a build-side outer join;
			// there is no need to create a hash table since there are no probe-side records

			List<MemorySegment> memory = getTwoBuffer();

			this.currentSpilledBuildSide = this.ioManager.createBlockChannelReader(p.getBuildSideChannel().getChannelID());
			final ChannelReaderInputView inView = new HeaderlessChannelReaderInputView(currentSpilledBuildSide, memory,
					p.getBuildSideBlockCount(), p.getLastSegmentLimit(), false);

			this.buildIterator = new WrappedRowIterator<>(
					new PagedChannelReaderInputViewIterator<>(inView,
							this.availableMemory, this.binaryBuildSideSerializer),
					binaryBuildSideSerializer.createInstance());

			this.partitionsPending.remove(0);

			return true;
		}

		this.probeMatchedPhase = true;
		this.buildIterVisited = false;

		// build the next table; memory must be allocated after this call
		buildTableFromSpilledPartition(p);

		// set the probe side - gather memory segments for reading
		LinkedBlockingQueue<MemorySegment> returnQueue = new LinkedBlockingQueue<>();
		this.currentSpilledProbeSide = this.ioManager.createBlockChannelReader(p.getProbeSideChannel().getChannelID(), returnQueue);

		List<MemorySegment> memory = getTwoBuffer();

		ChannelReaderInputViewIterator<BinaryRow> probeReader = new ChannelReaderInputViewIterator<>(this.currentSpilledProbeSide,
				returnQueue, memory, this.availableMemory, this.binaryProbeSideSerializer, p.getProbeSideBlockCount());
		this.probeIterator.set(probeReader);
		this.probeIterator.setReuse(binaryProbeSideSerializer.createInstance());

		// unregister the pending partition
		this.partitionsPending.remove(0);
		this.currentRecursionDepth = p.getRecursionLevel() + 1;

		// recursively get the next
		return nextMatching();
	}

	/**
	 * Closes the hash table. This effectively releases all internal structures and closes all
	 * open files and removes them. The call to this method is valid both as a cleanup after the
	 * complete inputs were properly processed, and as an cancellation call, which cleans up
	 * all resources that are currently held by the hash join.
	 */
	public void close() {
		// make sure that we close only once
		if (!this.closed.compareAndSet(false, true)) {
			return;
		}

		// clear the iterators, so the next call to next() will notice
		this.bucketIterator = null;
		this.probeIterator = null;

		// clear the memory in the partitions
		clearPartitions();

		// clear the current probe side channel, if there is one
		if (this.currentSpilledProbeSide != null) {
			try {
				this.currentSpilledProbeSide.closeAndDelete();
			} catch (Throwable t) {
				LOG.warn("Could not close and delete the temp file for the current spilled partition probe side.", t);
			}
		}

		// clear the partitions that are still to be done (that have files on disk)
		for (final BinaryHashPartition p : this.partitionsPending) {
			p.clearAllMemory(this.availableMemory);
		}

		// return the write-behind buffers
		for (int i = 0; i < this.numWriteBehindBuffers + this.writeBehindBuffersAvailable; i++) {
			try {
				this.availableMemory.add(this.writeBehindBuffers.take());
			} catch (InterruptedException iex) {
				throw new RuntimeException("Hashtable closing was interrupted");
			}
		}
		this.writeBehindBuffersAvailable = 0;
	}

	public void free() {
		if (this.closed.get()) {
			if (allocatedFloatingNum > 0) {
				MemUtil.releaseSpecificNumFloatingSegments(memManager, availableMemory, allocatedFloatingNum);
				allocatedFloatingNum = 0;
			}
			memManager.release(availableMemory);
		} else {
			throw new IllegalStateException("Cannot release memory until BinaryHashTable is closed!");
		}
	}

	@VisibleForTesting
	List<MemorySegment> getFreedMemory() {
		return this.availableMemory;
	}

	public void free(MemorySegment segment) {
		this.availableMemory.add(segment);
	}

	/**
	 * Bucket area need at-least one and data need at-least one.
	 * In the initialization phase, we can use (totalNumBuffers - numWriteBehindBuffers) Segments.
	 * However, in the buildTableFromSpilledPartition phase, only (totalNumBuffers - numWriteBehindBuffers - 2)
	 * can be used because two Buffers are needed to read the data.
	 */
	private int maxNumPartition() {
		return (availableMemory.size() + writeBehindBuffersAvailable) / 2;
	}

	/**
	 * Give up to one-sixth of the memory of the bucket area.
	 */
	int maxInitBufferOfBucketArea(int partitions) {
		return Math.max(1, ((reservedNumBuffers - numWriteBehindBuffers - 2) / 6) / partitions);
	}

	private void buildTableFromSpilledPartition(
			final BinaryHashPartition p) throws IOException {

		final int nextRecursionLevel = p.getRecursionLevel() + 1;
		if (nextRecursionLevel == 2) {
			LOG.info("Recursive hash join: partition number is " + p.getPartitionNumber());
		} else if (nextRecursionLevel > MAX_RECURSION_DEPTH) {
			throw new RuntimeException("Hash join exceeded maximum number of recursions, without reducing "
					+ "partitions enough to be memory resident. Probably cause: Too many duplicate keys.");
		}

		if (p.getBuildSideBlockCount() > p.getProbeSideBlockCount()) {
			LOG.info(String.format(
					"Hash join: Partition(%d) " +
							"build side block [%d] more than probe side block [%d]",
					p.getPartitionNumber(),
					p.getBuildSideBlockCount(),
					p.getProbeSideBlockCount()));
		}

		// we distinguish two cases here:
		// 1) The partition fits entirely into main memory. That is the case if we have enough buffers for
		//    all partition segments, plus enough buffers to hold the table structure.
		//    --> We read the partition in as it is and create a hashtable that references only
		//        that single partition.
		// 2) We can not guarantee that enough memory segments are available and read the partition
		//    in, distributing its data among newly created partitions.
		final int totalBuffersAvailable = this.availableMemory.size() + this.writeBehindBuffersAvailable;
		if (totalBuffersAvailable != this.reservedNumBuffers + this.allocatedFloatingNum - this.numWriteBehindBuffers) {
			throw new RuntimeException("Hash Join bug in memory management: Memory buffers leaked.");
		}

		long numBuckets = p.getBuildSideRecordCount() / BinaryHashBucketArea.NUM_ENTRIES_PER_BUCKET + 1;

		// we need to consider the worst case where everything hashes to one bucket which needs to overflow by the same
		// number of total buckets again. Also, one buffer needs to remain for the probing
		int maxBucketAreaBuffers = Math.max((int) (2 * (numBuckets / (this.bucketsPerSegmentMask + 1))), 1);
		final long totalBuffersNeeded = maxBucketAreaBuffers + p.getBuildSideBlockCount() + 2;

		if (totalBuffersNeeded < totalBuffersAvailable) {
			LOG.info(String.format("Build in memory hash table from spilled partition [%d]", p.getPartitionNumber()));

			// we are guaranteed to stay in memory
			ensureNumBuffersReturned(p.getBuildSideBlockCount());

			// first read the partition in
			final BulkBlockChannelReader reader = this.ioManager.createBulkBlockChannelReader(p.getBuildSideChannel().getChannelID(),
					this.availableMemory, p.getBuildSideBlockCount());

			reader.closeAndDelete();

			final List<MemorySegment> partitionBuffers = reader.getFullSegments();
			BinaryHashBucketArea area = new BinaryHashBucketArea(this, (int) p.getBuildSideRecordCount(), maxBucketAreaBuffers);
			final BinaryHashPartition newPart = new BinaryHashPartition(area, this.binaryBuildSideSerializer, this.binaryProbeSideSerializer,
					0, nextRecursionLevel, partitionBuffers, p.getBuildSideRecordCount(), this.segmentSize, p.getLastSegmentLimit());
			area.setPartition(newPart);

			this.partitionsBeingBuilt.add(newPart);

			// now, index the partition through a hash table
			final BinaryHashPartition.PartitionIterator pIter = newPart.getPartitionIterator(this.buildSideProjection);

			while (pIter.hasNext()) {
				final int hashCode = hash(pIter.getCurrentHashCode(), nextRecursionLevel);
				final int pointer = (int) pIter.getPointer();
				area.insertToBucket(hashCode, pointer, false, true);
			}
		} else {
			// go over the complete input and insert every element into the hash table
			// first set up the reader with some memory.
			final List<MemorySegment> segments = new ArrayList<>(2);
			segments.add(getNotNullNextBuffer());
			segments.add(getNotNullNextBuffer());

			// compute in how many splits, we'd need to partition the result
			final int splits = (int) (totalBuffersNeeded / totalBuffersAvailable) + 1;
			final int partitionFanOut = Math.min(Math.min(10 * splits, MAX_NUM_PARTITIONS), maxNumPartition());

			createPartitions(partitionFanOut, nextRecursionLevel);
			LOG.info(String.format("Build hybrid hash table from spilled partition [%d] with recursion level [%d]",
					p.getPartitionNumber(), nextRecursionLevel));

			final BlockChannelReader<MemorySegment> inReader = this.ioManager.createBlockChannelReader(p.getBuildSideChannel().getChannelID());
			final ChannelReaderInputView inView = new HeaderlessChannelReaderInputView(inReader, segments,
					p.getBuildSideBlockCount(), p.getLastSegmentLimit(), false);
			final PagedChannelReaderInputViewIterator<BinaryRow> inIter = new PagedChannelReaderInputViewIterator<>(inView,
					this.availableMemory, this.binaryBuildSideSerializer);
			BinaryRow rec = this.binaryBuildSideSerializer.createInstance();
			while ((rec = inIter.next(rec)) != null) {
				final int hashCode = hash(this.buildSideProjection.apply(rec).hashCode(), nextRecursionLevel);
				insertIntoTable(rec, hashCode);
			}

			inReader.closeAndDelete();

			// finalize the partitions
			for (BinaryHashPartition part : this.partitionsBeingBuilt) {
				part.finalizeBuildPhase(this.ioManager, this.currentEnumerator, this.writeBehindBuffers);
			}
		}
	}

	private void insertIntoTable(final BinaryRow record, final int hashCode) throws IOException {
		BinaryHashPartition p = partitionsBeingBuilt.get(hashCode % partitionsBeingBuilt.size());
		if (p.isInMemory()) {
			if (!p.bucketArea.appendRecordAndInsert(record, hashCode)) {
				p.addHashBloomFilter(hashCode);
			}
		} else {
			p.insertIntoBuildBuffer(record);
			p.addHashBloomFilter(hashCode);
		}
	}

	private MemorySegment getNotNullNextBuffer() {
		MemorySegment buffer = getNextBuffer();
		if (buffer == null) {
			throw new RuntimeException("Bug in HybridHashJoin: No memory became available.");
		}
		return buffer;
	}

	private void createPartitions(int numPartitions, int recursionLevel) {
		// sanity check
		ensureNumBuffersReturned(numPartitions);

		this.currentEnumerator = this.ioManager.createChannelEnumerator();

		this.partitionsBeingBuilt.clear();
		double numRecordPerPartition = (double) buildRowCount / numPartitions;
		int maxBuffer = maxInitBufferOfBucketArea(numPartitions);
		for (int i = 0; i < numPartitions; i++) {
			BinaryHashBucketArea area = new BinaryHashBucketArea(this, numRecordPerPartition, maxBuffer);
			BinaryHashPartition p = new BinaryHashPartition(area, this.binaryBuildSideSerializer,
					this.binaryProbeSideSerializer, i, recursionLevel, getNotNullNextBuffer(), this, this.segmentSize);
			area.setPartition(p);
			this.partitionsBeingBuilt.add(p);
		}
	}

	/**
	 * This method clears all partitions currently residing (partially) in memory. It releases all
	 * memory
	 * and deletes all spilled partitions.
	 *
	 * <p>This method is intended for a hard cleanup in the case that the join is aborted.
	 */
	private void clearPartitions() {
		for (int i = this.partitionsBeingBuilt.size() - 1; i >= 0; --i) {
			final BinaryHashPartition p = this.partitionsBeingBuilt.get(i);
			try {
				p.clearAllMemory(this.availableMemory);
			} catch (Exception e) {
				LOG.error("Error during partition cleanup.", e);
			}
		}
		this.partitionsBeingBuilt.clear();
	}

	/**
	 * Selects a partition and spills it. The number of the spilled partition is returned.
	 *
	 * @return The number of the spilled partition.
	 */
	int spillPartition() throws IOException {
		// find the largest partition
		int largestNumBlocks = 0;
		int largestPartNum = -1;

		for (int i = 0; i < partitionsBeingBuilt.size(); i++) {
			BinaryHashPartition p = partitionsBeingBuilt.get(i);
			if (p.isInMemory() && p.getNumOccupiedMemorySegments() > largestNumBlocks) {
				largestNumBlocks = p.getNumOccupiedMemorySegments();
				largestPartNum = i;
			}
		}
		final BinaryHashPartition p = partitionsBeingBuilt.get(largestPartNum);

		// spill the partition
		int numBuffersFreed = p.spillPartition(this.ioManager,
				this.currentEnumerator.next(), this.writeBehindBuffers);
		this.writeBehindBuffersAvailable += numBuffersFreed;

		LOG.info(String.format("Grace hash join: Ran out memory, choosing partition " +
						"[%d] to spill, %d memory segments being freed",
				largestPartNum, numBuffersFreed));

		// grab as many buffers as are available directly
		MemorySegment currBuff;
		while (this.writeBehindBuffersAvailable > 0 && (currBuff = this.writeBehindBuffers.poll()) != null) {
			this.availableMemory.add(currBuff);
			this.writeBehindBuffersAvailable--;
		}
		numSpillFiles++;
		spillInBytes += numBuffersFreed * segmentSize;
		// The bloomFilter is built after the data is spilled, so that we can use enough memory.
		p.buildBloomFilterAndFreeBucket();
		return largestPartNum;
	}

	/**
	 * This method makes sure that at least a certain number of memory segments is in the list of
	 * free segments.
	 * Free memory can be in the list of free segments, or in the return-queue where segments used
	 * to write behind are
	 * put. The number of segments that are in that return-queue, but are actually reclaimable is
	 * tracked. This method
	 * makes sure at least a certain number of buffers is reclaimed.
	 *
	 * @param minRequiredAvailable The minimum number of buffers that needs to be reclaimed.
	 */
	void ensureNumBuffersReturned(final int minRequiredAvailable) {
		if (minRequiredAvailable > this.availableMemory.size() + this.writeBehindBuffersAvailable) {
			throw new IllegalArgumentException("More buffers requested available than totally available.");
		}

		try {
			while (this.availableMemory.size() < minRequiredAvailable) {
				this.availableMemory.add(this.writeBehindBuffers.take());
				this.writeBehindBuffersAvailable--;
			}
		} catch (InterruptedException iex) {
			throw new RuntimeException("Hash Join was interrupted.");
		}
	}

	/**
	 * Gets the next buffer to be used with the hash-table, either for an in-memory partition, or
	 * for the table buckets. This method returns <tt>null</tt>, if no more buffer is available.
	 * Spilling a partition may free new buffers then.
	 *
	 * @return The next buffer to be used by the hash-table, or null, if no buffer remains.
	 */
	MemorySegment getNextBuffer() {
		// check if the list directly offers memory
		int s = this.availableMemory.size();
		if (s > 0) {
			return this.availableMemory.remove(s - 1);
		}

		// check if there are write behind buffers that actually are to be used for the hash table
		if (this.writeBehindBuffersAvailable > 0) {
			// grab at least one, no matter what
			MemorySegment toReturn;
			try {
				toReturn = this.writeBehindBuffers.take();
			} catch (InterruptedException iex) {
				throw new RuntimeException("Hybrid Hash Join was interrupted while taking a buffer.");
			}
			this.writeBehindBuffersAvailable--;

			// grab as many more buffers as are available directly
			MemorySegment currBuff;
			while (this.writeBehindBuffersAvailable > 0 && (currBuff = this.writeBehindBuffers.poll()) != null) {
				this.availableMemory.add(currBuff);
				this.writeBehindBuffersAvailable--;
			}
			return toReturn;
		} else {
			if (reservedNumBuffers + allocatedFloatingNum >= preferedNumBuffers) {
				//no more memory.
				return null;
			} else {
				int requestNum = Math.min(perRequestNumBuffers, preferedNumBuffers - reservedNumBuffers -
						allocatedFloatingNum);
				//apply for much more memory.
				try {
					List<MemorySegment> allocates = memManager.allocatePages(owner, requestNum, false);
					this.availableMemory.addAll(allocates);
					allocatedFloatingNum += allocates.size();
					allocates.clear();
					LOG.info("{} allocate {} floating segments successfully!", owner, requestNum);
				} catch (MemoryAllocationException e) {
					LOG.warn("BinaryHashMap can't allocate {} floating pages, and now used {} pages",
							requestNum, reservedNumBuffers + allocatedFloatingNum, e);
					//can't allocate much more memory.
					return null;
				}
				if (this.availableMemory.size() > 0) {
					return this.availableMemory.remove(this.availableMemory.size() - 1);
				} else {
					return null;
				}
			}
		}
	}

	/**
	 * Bulk memory acquisition.
	 * NOTE: Failure to get memory will throw an exception.
	 */
	MemorySegment[] getNextBuffers(int bufferSize) {
		MemorySegment[] memorySegments = new MemorySegment[bufferSize];
		for (int i = 0; i < bufferSize; i++) {
			MemorySegment nextBuffer = getNextBuffer();
			if (nextBuffer == null) {
				throw new RuntimeException("No enough buffers!");
			}
			memorySegments[i] = nextBuffer;
		}
		return memorySegments;
	}

	/**
	 * This is the method called by the partitions to request memory to serialize records.
	 * It automatically spills partitions, if memory runs out.
	 *
	 * @return The next available memory segment.
	 */
	@Override
	public MemorySegment nextSegment() {
		final MemorySegment seg = getNextBuffer();
		if (seg != null) {
			return seg;
		} else {
			try {
				spillPartition();
			} catch (IOException ioex) {
				throw new RuntimeException("Error spilling Hash Join Partition" + (ioex.getMessage() == null ?
						"." : ": " + ioex.getMessage()), ioex);
			}

			MemorySegment fromSpill = getNextBuffer();
			if (fromSpill == null) {
				throw new RuntimeException("BUG in Hybrid Hash Join: Spilling did not free a buffer.");
			} else {
				return fromSpill;
			}
		}
	}

	boolean applyCondition(BinaryRow candidate) {
		BinaryRow buildKey = buildSideProjection.apply(candidate);
		// They come from Projection, so we can make sure it is in byte[].
		boolean equal = buildKey.getSizeInBytes() == probeKey.getSizeInBytes()
				&& BinaryRowUtil.byteArrayEquals(
					buildKey.getMemorySegment().getHeapMemory(),
					probeKey.getMemorySegment().getHeapMemory(),
					buildKey.getSizeInBytes());
		if (!nullSafe) {
			equal = equal && !(filterAllNulls ? buildKey.anyNull() : buildKey.anyNull(nullFilterKeys));
		}
		return condFunc == null ? equal : equal && (reverseJoin ? condFunc.apply(probeRow, candidate)
				: condFunc.apply(candidate, probeRow));
	}

	@Override
	public int pageSize() {
		return segmentSize;
	}

	@Override
	public void returnAll(List<MemorySegment> memory) {
		for (MemorySegment segment : memory) {
			if (segment != null) {
				availableMemory.add(segment);
			}
		}
	}

	@Override
	public void clear() {
		throw new RuntimeException("Should not be invoked.");
	}

	@Override
	public int remainBuffers() {
		return availableMemory.size() + writeBehindBuffersAvailable;
	}

	public long getUsedMemoryInBytes() {
		return (reservedNumBuffers + allocatedFloatingNum - availableMemory.size()) *
				((long) memManager.getPageSize());
	}

	public long getNumSpillFiles() {
		return numSpillFiles;
	}

	public long getSpillInBytes() {
		return spillInBytes;
	}
}
