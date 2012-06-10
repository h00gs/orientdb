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
package com.orientechnologies.orient.server.cluster.log;

import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;

/**
 * Write all the operation during server cluster.<br/>
 * Uses the classic IO API and NOT the MMAP to avoid the buffer is not buffered by OS.<br/>
 * <br/>
 * Record structure:<br/>
 * <code>
 * +------------------------------------------------+<br/>
 * |.......... FIXED SIZE AREA = 19 bytes ..........|<br/>
 * +--------+------------+----------------+---------+<br/>
 * | OPERAT | CLUSTER ID | CLUSTER OFFSET | DATE .. |<br/>
 * | 1 byte | 2 bytes .. | 8 bytes ...... | 8 bytes |<br/>
 * +--------|------------+----------------+---------+<br/>
 * = 19 bytes
 * </code><br/>
 */
public class OReplicationLog extends OSingleFileSegment {
  /**
   * 
   */
  public static final String              REPLICATION_DIRECTORY = "${" + Orient.ORIENTDB_HOME + "}/replication/";
  public static final String              EXTENSION             = ".dol";
  private static final int                DEF_START_SIZE        = 262144;

  private static final int                OFFSET_OPERAT         = 0;
  private static final int                OFFSET_RID            = OFFSET_OPERAT + OBinaryProtocol.SIZE_BYTE;
  private static final int                OFFSET_DATE           = OFFSET_RID + ORecordId.PERSISTENT_SIZE;
  private static final int                RECORD_SIZE           = OFFSET_DATE + OBinaryProtocol.SIZE_LONG;

  private boolean                         synchEnabled;
  private OSharedResourceAdaptiveExternal lock                  = new OSharedResourceAdaptiveExternal(
                                                                    OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(),
                                                                    0, true);
  private final long                      limit;

  public OReplicationLog(final String iNodeId, final String iDatabase, final long iLimit) throws IOException {
    super(REPLICATION_DIRECTORY + iDatabase + "/" + iNodeId.replace('.', '_').replace(':', '-') + EXTENSION,
        OGlobalConfiguration.DISTRIBUTED_LOG_TYPE.getValueAsString());
    init();
    limit = iLimit;
  }

  protected void init() throws IOException {
    synchEnabled = OGlobalConfiguration.DISTRIBUTED_LOG_SYNCH.getValueAsBoolean();

    file.setFailCheck(false);
    if (exists()) {
      open();
    } else
      create(DEF_START_SIZE);
  }

  @Override
  public boolean open() throws IOException {
    final boolean result = super.open();
    return result;
  }

  @Override
  public void create(final int iStartSize) throws IOException {
    super.create(iStartSize);
  }

  /**
   * Reset the file
   */
  public void resetIfEmpty(final int iLast) throws IOException {
    if (totalEntries() == iLast)
      // NO CONCURRENT ENTRIES, RESET IT
      file.shrink(0);
  }

  public void reset() throws IOException {
    file.shrink(0);
  }

  /**
   * Appends a log entry until reach the configured limit if any (>-1)
   */
  public long appendLog(final byte iOperation, final ORecordId iRID) throws IOException {

    lock.acquireExclusiveLock();
    try {
      final long serial = totalEntries();

      if (limit > -1 && serial > limit)
        return -1;

      if (serial > 0)
        OLogManager.instance().warn(this, "Journaled operation #%d as %s against record %s", serial,
            ORecordOperation.getName(iOperation), iRID);
      else
        OLogManager.instance().debug(this, "Journaled operation #%d as %s against record %s", serial,
            ORecordOperation.getName(iOperation), iRID);

      int offset = file.allocateSpace(RECORD_SIZE);

      file.writeByte(offset, iOperation);
      offset += OBinaryProtocol.SIZE_BYTE;

      file.writeShort(offset, (short) iRID.clusterId);
      offset += OBinaryProtocol.SIZE_SHORT;

      file.writeLong(offset, iRID.clusterPosition);
      offset += OBinaryProtocol.SIZE_LONG;

      file.writeLong(offset, System.currentTimeMillis());

      if (synchEnabled)
        file.synch();

      return serial;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public ORecordOperation getEntry(final int iPosition, final ORecordOperation iEntry) throws IOException {
    lock.acquireExclusiveLock();
    try {

      final int pos = iPosition * RECORD_SIZE;

      iEntry.type = file.readByte(pos);
      iEntry.record = new ORecordId(file.readShort(pos + OFFSET_RID), file.readLong(pos + OFFSET_RID + OBinaryProtocol.SIZE_SHORT));
      iEntry.date = file.readLong(pos + OFFSET_DATE);
      return iEntry;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Resets the current entry to avoid re-sending if align interrupts for any reason
   * 
   * @param iPosition
   * @throws IOException
   */
  public void resetEntry(final int iPosition) throws IOException {
    lock.acquireExclusiveLock();
    try {

      final int pos = iPosition * RECORD_SIZE;

      file.writeByte(pos, (byte) -1);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean isEmpty() {
    return file.getFilledUpTo() == 0;
  }

  public int totalEntries() {
    return file.getFilledUpTo() / RECORD_SIZE;
  }
}