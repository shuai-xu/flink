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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.InternalType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Serializer for {@link BinaryRow}.
 */
public class BinaryRowSerializer extends AbstractRowSerializer<BinaryRow> {

	private static final Logger LOG = LoggerFactory.getLogger(BinaryRowSerializer.class);

	private static final long serialVersionUID = 1L;
	public static final int LENGTH_SIZE_IN_BYTES = 4;

	private final int fixedLengthPartSize;

	public BinaryRowSerializer() {
		this(new InternalType[0]);
	}

	public BinaryRowSerializer(InternalType... types) {
		this(DataTypes.toTypeInfos(types));
	}

	public BinaryRowSerializer(TypeInformation<?>... types) {
		super(types);
		this.fixedLengthPartSize = BinaryRow.calculateFixPartSizeInBytes(numFields);
	}

	@Override
	public BinaryRow baseRowToBinary(BinaryRow baseRow) throws IOException {
		return baseRow;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<BinaryRow> duplicate() {
		return new BinaryRowSerializer(types);
	}

	@Override
	public BinaryRow createInstance() {
		return new BinaryRow(numFields);
	}

	@Override
	public BinaryRow copy(BinaryRow from) {
		return copy(from, new BinaryRow(numFields));
	}

	// ============================ serialize ===================================

	@Override
	public BinaryRow copy(BinaryRow from, BinaryRow reuse) {
		return from.copy(reuse);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BinaryRow record, DataOutputView target) throws IOException {
		int sizeInBytes = record.getSizeInBytes();
		target.writeInt(sizeInBytes);
		int offset = record.getBaseOffset();
		for (MemorySegment segment : record.getAllSegments()) {
			int remain = segment.size() - offset;
			int copySize = remain > sizeInBytes ? sizeInBytes : remain;
			target.write(segment, offset, copySize);

			sizeInBytes -= copySize;
			offset = 0;
		}
		if (sizeInBytes != 0) {
			throw new RuntimeException("No copy finished, this should be a bug, " +
					"The remaining length is: " + sizeInBytes);
		}
	}

	// ============================ serializeToPages ===================================

	/**
	 * TODO now stream runtime force use non-reuse version.
	 */
	@Override
	public BinaryRow deserialize(DataInputView source) throws IOException {
		return getBinaryRow(source, numFields);
	}

	public static BinaryRow getBinaryRow(DataInputView source, int numFields) throws IOException {
		BinaryRow row = new BinaryRow(numFields);
		int length = source.readInt();
		byte[] bytes = new byte[length];
		try {
			source.readFully(bytes);
		} catch (EOFException e) {
			LOG.error("ReadFully fail, Length is: " + length);
			throw e;
		}
		row.pointTo(MemorySegmentFactory.wrap(bytes), 0, length);
		return row;
	}

	@Override
	public BinaryRow deserialize(BinaryRow reuse, DataInputView source) throws IOException {
		MemorySegment segment = reuse.getMemorySegment();
		checkArgument(segment == null ||
				(reuse.getAllSegments().length == 1 && reuse.getBaseOffset() == 0));

		int length = source.readInt();
		if (segment == null || segment.size() < length) {
			segment = MemorySegmentFactory.wrap(new byte[length]);
		}
		source.readFully(segment.getHeapMemory(), 0, length);
		reuse.pointTo(segment, 0, length);
		return reuse;
	}

	@Override
	public int serializeToPages(BinaryRow record,
			AbstractPagedOutputView target) throws IOException {

		checkArgument(target.getHeaderLength() == 0);

		int sizeInBytes = record.getSizeInBytes();
		int currSegAvailable = target.getSegmentSize() - target.getCurrentPositionInSegment();

		if (record.getAllSegments().length == 1 &&
				currSegAvailable >= getSerializedRowFixedPartLength()) {
			target.writeInt(sizeInBytes);
			target.write(record.getMemorySegment(), record.getBaseOffset(), sizeInBytes);
			return 0;
		} else {
			return serializeToPagesSlow(record, target, currSegAvailable);
		}
	}

	private int serializeToPagesSlow(
			BinaryRow record, AbstractPagedOutputView target,
			int currSegAvailable) throws IOException {
		int skip = 0;

		if (currSegAvailable < getSerializedRowFixedPartLength()) {
			skip = currSegAvailable;
			target.advance();
		}

		target.writeInt(record.getSizeInBytes());

		int len = record.getSizeInBytes();
		int point = record.getBaseOffset();
		int restOfCurrSeg = record.getMemorySegment().size() - point;
		int needCopy;
		for (MemorySegment ms : record.getAllSegments()) {
			needCopy = restOfCurrSeg > len ? len : restOfCurrSeg;
			len -= needCopy;
			while (needCopy > 0) {
				target.checkAdvance();
				int outSize = (target.getSegmentSize() - target.getCurrentPositionInSegment());
				int currCopy = needCopy > outSize ? outSize : needCopy;
				ms.copyTo(point, target.getCurrentSegment(), target.getCurrentPositionInSegment(), currCopy);
				target.skipBytesToWrite(currCopy);
				point += currCopy;
				needCopy -= currCopy;
			}
			point = 0;
			restOfCurrSeg = ms.size();
		}
		checkArgument(len == 0);

		return skip;
	}

	// ============================ deserializeFromPages ===================================

	@Override
	public BinaryRow deserializeFromPages(AbstractPagedInputView source) throws IOException {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public BinaryRow deserializeFromPages(BinaryRow reuse,
		AbstractPagedInputView source) throws IOException {
		checkSkipRead(source);
		return deserialize(reuse, source);
	}

	@Override
	public BinaryRow mapFromPages(AbstractPagedInputView source) throws IOException {
		BinaryRow row = createInstance();
		pointTo(row, source);
		return row;
	}

	@Override
	public BinaryRow mapFromPages(BinaryRow reuse,
			AbstractPagedInputView source) throws IOException {
		pointTo(reuse, source);
		return reuse;
	}

	private void pointTo(BinaryRow row, AbstractPagedInputView source) throws IOException {

		checkArgument(source.getHeaderLength() == 0);

		// skip if there is no enough size.
		checkSkipRead(source);

		int length = source.readInt();

		int currSegAvailable = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
		MemorySegment currSeg = source.getCurrentSegment();
		int currPosInSeg = source.getCurrentPositionInSegment();
		if (currSegAvailable >= length) {
			// all in one segment, that's good.
			row.pointTo(currSeg, currPosInSeg, length);
			source.skipBytesToRead(length);
		} else {
			pointToSlow(row, length, source, currSegAvailable, currSeg, currPosInSeg);
		}
	}

	private void pointToSlow(
			BinaryRow row, int length, AbstractPagedInputView source,
			int currSegAvailable, MemorySegment currSeg, int currPosInSeg) throws IOException {

		int restSize = length - currSegAvailable;

		int segmentSize = currSeg.size();
		int div = restSize / segmentSize;
		int rem = restSize - segmentSize * div; // equal to p % q
		int varSegSize = rem == 0 ? div : div + 1;

		MemorySegment[] segments = new MemorySegment[varSegSize + 1];
		segments[0] = currSeg;
		for (int i = 0; i < varSegSize; i++) {
			source.advance();
			segments[i + 1] = source.getCurrentSegment();
			if (i == varSegSize - 1 && rem != 0) {
				source.skipBytesToRead(rem);
			}
		}
		row.pointTo(segments, currPosInSeg, length);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	public void copyFromPagesToView(
			AbstractPagedInputView source, DataOutputView target) throws IOException {
		checkSkipRead(source);
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof BinaryRowSerializer
				&& Arrays.equals(types, ((BinaryRowSerializer) obj).types);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BinaryRowSerializer;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.types);
	}

	/**
	 * Return fixed part length to serialize one row.
	 */
	public int getSerializedRowFixedPartLength() {
		return fixedLengthPartSize + LENGTH_SIZE_IN_BYTES;
	}

	/**
	 * Return if the rows serialized by this serializer are of fixed length.
	 */
	public boolean isRowFixedLength() {
		for (TypeInformation type : getTypes()) {
			if (!BinaryRow.isFixedLength(DataTypes.internal(type))) {
				return false;
			}
		}
		return true;
	}

	public void checkSkipRead(AbstractPagedInputView source) throws IOException {
		// skip if there is no enough size.
		int available = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
		if (available < getSerializedRowFixedPartLength()) {
			source.advance();
		}
	}
}
