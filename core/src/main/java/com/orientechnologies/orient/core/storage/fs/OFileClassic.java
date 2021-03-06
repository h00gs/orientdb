/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

/**
 * Need to be synchronized by the external. Multiple Reader, Single Writer.<br/>
 * Header structure:<br/>
 * <br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * | | | SOFTLY CLOSED | SECURITY CODE |<br/>
 * | 8 bytes . | 8 bytes .... | 1 byte ...... | 32 bytes .... |<br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * = 1024 bytes<br/>
 * <br/>
 */
public class OFileClassic extends OAbstractFile {
  public final static String NAME                = "classic";
  protected ByteBuffer       internalWriteBuffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_LONG);
  private AtomicBoolean      initCompleted       = new AtomicBoolean(false);

  public OFileClassic init(String iFileName, String iMode) {
    if (initCompleted.compareAndSet(false, true)) {
      super.init(iFileName, iMode);
    }
    return this;
  }

  @Override
  public void close() throws IOException {
    acquireWriteLock();
    try {
      if (channel != null)
        setSoftlyClosed(true);

      super.close();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public int allocateSpace(int iSize) throws IOException {
    final int currentSize = getFilledUpTo();
    if (maxSize > 0 && currentSize + iSize > maxSize)
      throw new IllegalArgumentException("Cannot enlarge file since the configured max size ("
          + OFileUtils.getSizeAsString(maxSize) + ") was reached! " + toString());

    size += iSize;
    return currentSize;
  }

  @Override
  public void shrink(int iSize) throws IOException {
    acquireWriteLock();
    try {
      channel.truncate(HEADER_SIZE + iSize);
      size = iSize;
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public int getFileSize() {
    return size;
  }

  @Override
  public int getFilledUpTo() {
    return size;
  }

  public void read(long iOffset, byte[] iData, int iLength, int iArrayOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, iLength);

      ByteBuffer buffer = ByteBuffer.wrap(iData, iArrayOffset, iLength);
      channel.read(buffer, iOffset);
    } finally {
      releaseReadLock();
    }
  }

  public void write(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException {
    acquireWriteLock();
    try {
      writeInternal(iOffset, iData, iSize, iArrayOffset);
    } finally {
      releaseWriteLock();
    }
  }

  private void writeInternal(long iOffset, byte[] iData, int iSize, int iArrayOffset) throws IOException {
    if (iData != null) {
      iOffset += HEADER_SIZE;
      ByteBuffer byteBuffer = ByteBuffer.wrap(iData, iArrayOffset, iSize);
      channel.write(byteBuffer, iOffset);
      setDirty();
    }
  }

  @Override
  public void read(long iOffset, byte[] iDestBuffer, int iLenght) throws IOException {
    read(iOffset, iDestBuffer, iLenght, 0);
  }

  @Override
  public int readInt(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_INT);
      return readData(iOffset, OBinaryProtocol.SIZE_INT).getInt();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public long readLong(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_LONG);
      return readData(iOffset, OBinaryProtocol.SIZE_LONG).getLong();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public short readShort(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_SHORT);
      return readData(iOffset, OBinaryProtocol.SIZE_SHORT).getShort();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public byte readByte(long iOffset) throws IOException {
    acquireReadLock();
    try {
      iOffset = checkRegions(iOffset, OBinaryProtocol.SIZE_BYTE);
      return readData(iOffset, OBinaryProtocol.SIZE_BYTE).get();
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void writeInt(long iOffset, final int iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_INT);
      buffer.putInt(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeLong(long iOffset, final long iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_LONG);
      buffer.putLong(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeShort(long iOffset, final short iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_SHORT);
      buffer.putShort(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public void writeByte(long iOffset, final byte iValue) throws IOException {
    acquireWriteLock();
    try {
      iOffset += HEADER_SIZE;
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_BYTE);
      buffer.put(iValue);
      writeBuffer(buffer, iOffset);
      setDirty();
    } finally {
      releaseWriteLock();
    }

  }

  @Override
  public void write(long iOffset, final byte[] iSourceBuffer) throws IOException {
    acquireWriteLock();
    try {
      if (iSourceBuffer != null) {
        writeInternal(iOffset, iSourceBuffer, iSourceBuffer.length, 0);
      }
    } finally {
      releaseWriteLock();
    }

  }

  /**
   * Synchronizes the buffered changes to disk.
   * 
   * @throws IOException
   * 
   */
  @Override
  public void synch() throws IOException {
    acquireWriteLock();
    try {
      flushHeader();
    } finally {
      releaseWriteLock();
    }
  }

  protected void flushHeader() throws IOException {
    if (headerDirty || dirty) {
      headerDirty = dirty = false;
      channel.force(false);
    }
  }

  @Override
  public void create(int iStartSize) throws IOException {
    acquireWriteLock();
    try {
      super.create(HEADER_SIZE);
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  protected void init() throws IOException {
    size = (int) (osFile.length() - HEADER_SIZE);
  }

  @Override
  protected void setFilledUpTo(final int iValue) throws IOException {
    size = iValue;
  }

  @Override
  public void setSize(final int iSize) throws IOException {
  }

  @Override
  public void writeHeaderLong(final int iPosition, final long iValue) throws IOException {
    acquireWriteLock();
    try {
      final ByteBuffer buffer = getWriteBuffer(OBinaryProtocol.SIZE_LONG);
      buffer.putLong(iValue);
      writeBuffer(buffer, HEADER_DATA_OFFSET + iPosition);
      setHeaderDirty();
    } finally {
      releaseWriteLock();
    }
  }

  @Override
  public long readHeaderLong(final int iPosition) throws IOException {
    acquireReadLock();
    try {
      return readData(HEADER_DATA_OFFSET + iPosition, OBinaryProtocol.SIZE_LONG).getLong();
    } finally {
      releaseReadLock();
    }
  }

  public boolean isSoftlyClosed() throws IOException {
    return true;
  }

  public void setSoftlyClosed(final boolean iValue) throws IOException {
  }

  /**
   * ALWAYS ADD THE HEADER SIZE BECAUSE ON THIS TYPE IS ALWAYS NEEDED
   */
  @Override
  protected long checkRegions(final long iOffset, final int iLength) {
    return super.checkRegions(iOffset, iLength) + HEADER_SIZE;
  }

  private ByteBuffer readData(final long iOffset, final int iSize) throws IOException {
    ByteBuffer buffer = getBuffer(iSize);
    channel.read(buffer, iOffset);
    buffer.rewind();
    return buffer;
  }

  private void writeBuffer(final ByteBuffer iBuffer, final long iOffset) throws IOException {
    iBuffer.rewind();
    channel.write(iBuffer, iOffset);
  }

  private ByteBuffer getBuffer(final int iLenght) {
    return ByteBuffer.allocate(iLenght);
  }

  private ByteBuffer getWriteBuffer(final int iLenght) {
    setDirty();
    if (iLenght <= OBinaryProtocol.SIZE_LONG)
      // RECYCLE WRITE BYTE BUFFER SINCE WRITES ARE SYNCHRONIZED
      return (ByteBuffer) internalWriteBuffer.rewind();

    return getBuffer(iLenght);
  }
}
