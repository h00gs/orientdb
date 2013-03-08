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
package com.orientechnologies.orient.core.db.graph.edge;

import java.util.Collection;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.graph.OLabeledGraph.DIRECTION;
import com.orientechnologies.orient.core.db.graph.OAdaptivePropertyGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OPropertyGraphDatabase;
import com.orientechnologies.orient.core.db.graph.vertex.OVertex;
import com.orientechnologies.orient.core.db.graph.vertex.OVertexDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Bidirectional link managed as a pair of raw LINK objects.
 * 
 * @author Luca Garulli
 * 
 */
public class OBidirectionalEdgeDocument implements OBidirectionalEdge {
  public static final String EDGE_FIELD_OUT  = OAdaptivePropertyGraphDatabase.CONNECTION_OUT;
  public static final String EDGE_FIELD_IN   = OAdaptivePropertyGraphDatabase.CONNECTION_IN;

  public static final String EDGE_CLASS_NAME = "OGraphEdge";

  private OIdentifiable      record;

  public OBidirectionalEdgeDocument(OIdentifiable iEdgeRecord) {
    record = iEdgeRecord;
  }

  public OBidirectionalEdgeDocument(final OVertex iOut, final OVertex iIn) {
    final ODocument edge = new ODocument(EDGE_CLASS_NAME);
    edge.field(EDGE_FIELD_OUT, iOut);
    edge.field(EDGE_FIELD_IN, iIn);

    record = edge;
  }

  @Override
  public OVertex getOutVertex() {
    return new OVertexDocument((OIdentifiable) ((ODocument) record.getRecord()).field(EDGE_FIELD_OUT));
  }

  @Override
  public OVertex getInVertex() {
    return new OVertexDocument((OIdentifiable) ((ODocument) record.getRecord()).field(EDGE_FIELD_IN));
  }

  @Override
  public OIdentifiable getOutVertexAsRecord() {
    return ((ODocument) record.getRecord()).field(EDGE_FIELD_OUT);
  }

  @Override
  public OIdentifiable getInVertexAsRecord() {
    return ((ODocument) record.getRecord()).field(EDGE_FIELD_IN);
  }

  public boolean drop() {
    final ODocument edge = record.getRecord();
    if (edge == null)
      return false;

    final OPropertyGraphDatabase db = (OPropertyGraphDatabase) ODatabaseRecordThreadLocal.INSTANCE.get();

    final boolean safeMode = db.beginBlock();
    try {
      // OUT VERTEX
      final ODocument outVertex = edge.field(EDGE_FIELD_OUT);

      db.acquireWriteLock(outVertex);
      try {
        final String outFieldName = db.getOutVertexField(edge.getClassName());
        dropEdgeFromVertex(edge, outVertex, outFieldName, outVertex.field(outFieldName));
        db.save(outVertex);
      } finally {
        db.releaseWriteLock(outVertex);
      }

      // IN VERTEX
      final ODocument inVertex = edge.field(EDGE_FIELD_IN);

      db.acquireWriteLock(inVertex);
      try {
        final String inFieldName = db.getOutVertexField(edge.getClassName());
        dropEdgeFromVertex(edge, inVertex, inFieldName, inVertex.field(inFieldName));
        db.save(inVertex);
      } finally {
        db.releaseWriteLock(inVertex);
      }

      db.delete(edge);

      db.commitBlock(safeMode);

    } catch (RuntimeException e) {
      db.rollbackBlock(safeMode);
      throw e;
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private void dropEdgeFromVertex(final ODocument edge, final ODocument iVertex, final String iFieldName, final Object iFieldValue) {
    if (iFieldValue == null) {
      // NO EDGE? WARN
      OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s while removing the edge %s",
          iVertex.getIdentity(), iFieldName, record.getIdentity());

    } else if (iFieldValue instanceof OIdentifiable) {
      // FOUND A SINGLE ITEM: JUST REMOVE IT

      if (iFieldValue.equals(record))
        iVertex.field(iFieldName, (OIdentifiable) null);
      else
        // NO EDGE? WARN
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s link while removing the edge %s",
            iVertex.getIdentity(), iFieldName, record.getIdentity());

    } else if (iFieldValue instanceof OMVRBTreeRIDSet) {
      // ALREADY A SET: JUST REMOVE THE NEW EDGE
      if (!((OMVRBTreeRIDSet) iFieldValue).remove(edge))
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s set while removing the edge %s",
            iVertex.getIdentity(), iFieldName, record.getIdentity());
    } else if (iFieldValue instanceof Collection<?>) {
      // CONVERT COLLECTION IN TREE-SET AND REMOVE THE EDGE
      final OMVRBTreeRIDSet out = new OMVRBTreeRIDSet(iVertex, (Collection<OIdentifiable>) iFieldValue);
      if (!out.remove(edge))
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s collection while removing the edge %s",
            iVertex.getIdentity(), iFieldName, record.getIdentity());
      else
        iVertex.field(iFieldName, out);
    } else
      throw new IllegalStateException("Wrong type found in the field '" + iFieldName + "': " + iFieldValue.getClass());
  }

  public static OIdentifiable getVertex(final ODocument iEdge, final DIRECTION iDirection) {
    return iEdge.field(iDirection == DIRECTION.OUT ? EDGE_FIELD_OUT : EDGE_FIELD_IN);
  }
}
