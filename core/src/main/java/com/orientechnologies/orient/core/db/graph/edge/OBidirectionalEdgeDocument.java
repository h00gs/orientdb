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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.graph.OAdaptivePropertyGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OLabeledGraph.DIRECTION;
import com.orientechnologies.orient.core.db.graph.vertex.OVertex;
import com.orientechnologies.orient.core.db.graph.vertex.OVertexDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Bidirectional link managed as a pair of raw LINK objects.
 * 
 * @author Luca Garulli
 * 
 */
public class OBidirectionalEdgeDocument extends OAbstractEdge {
  public static final String EDGE_FIELD_OUT  = OAdaptivePropertyGraphDatabase.CONNECTION_OUT;
  public static final String EDGE_FIELD_IN   = OAdaptivePropertyGraphDatabase.CONNECTION_IN;

  public static final String EDGE_CLASS_NAME = "OGraphEdge";

  private OIdentifiable      record;

  public OBidirectionalEdgeDocument(OIdentifiable iEdgeRecord) {
    record = iEdgeRecord;
    checkClass();
  }

  public OBidirectionalEdgeDocument(final String iClassName, final OVertex iOut, final OVertex iIn) {
    final ODocument edge = new ODocument(iClassName);
    edge.field(EDGE_FIELD_OUT, iOut);
    edge.field(EDGE_FIELD_IN, iIn);

    record = edge;
    checkClass();
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

    final OAdaptivePropertyGraphDatabase db = (OAdaptivePropertyGraphDatabase) ODatabaseRecordThreadLocal.INSTANCE.get();

    final boolean safeMode = db.beginBlock();
    try {
      // OUT VERTEX
      final ODocument outVertex = edge.field(EDGE_FIELD_OUT);

      db.acquireWriteLock(outVertex);
      try {
        final String outFieldName = OVertexDocument.getConnectionFieldName(DIRECTION.OUT, edge.getClassName());
        dropEdgeFromVertex(edge, outVertex, outFieldName, outVertex.field(outFieldName));
        db.save(outVertex);
      } finally {
        db.releaseWriteLock(outVertex);
      }

      // IN VERTEX
      final ODocument inVertex = edge.field(EDGE_FIELD_IN);

      db.acquireWriteLock(inVertex);
      try {
        final String inFieldName = OVertexDocument.getConnectionFieldName(DIRECTION.IN, edge.getClassName());
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

  public static OIdentifiable getVertex(final ODocument iEdge, final DIRECTION iDirection) {
    return iEdge.field(iDirection == DIRECTION.OUT ? EDGE_FIELD_OUT : EDGE_FIELD_IN);
  }

  protected void checkClass() {
    // FORCE EARLY UNMARSHALLING
    final ODocument doc = record.getRecord();
    doc.deserializeFields();

    if (!doc.getSchemaClass().isSubClassOf(EDGE_CLASS_NAME))
      throw new IllegalArgumentException("The document received is not an edge. Found class '" + doc.getSchemaClass() + "'");
  }

}
