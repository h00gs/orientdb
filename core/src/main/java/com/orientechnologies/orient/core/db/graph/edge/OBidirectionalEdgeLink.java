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
import com.orientechnologies.orient.core.db.graph.OPropertyGraph;
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
public class OBidirectionalEdgeLink implements OBidirectionalEdge {
  protected final OIdentifiable out;
  protected final OIdentifiable in;
  protected final String        fieldName;

  public OBidirectionalEdgeLink(final OIdentifiable iOut, final OIdentifiable iIn, final String iFieldName) {
    this.out = iOut;
    this.in = iIn;
    this.fieldName = iFieldName;
  }

  @Override
  public OVertex getOutVertex() {
    if (out instanceof OVertex)
      return (OVertex) out;
    return new OVertexDocument(out);
  }

  @Override
  public OVertex getInVertex() {
    if (in instanceof OVertex)
      return (OVertex) in;
    return new OVertexDocument(in);
  }

  @Override
  public OIdentifiable getOutVertexAsRecord() {
    return out;
  }

  @Override
  public OIdentifiable getInVertexAsRecord() {
    return in;
  }

  public String getFieldName() {
    return fieldName;
  }

  public boolean drop() {
    final OAdaptivePropertyGraphDatabase db = (OAdaptivePropertyGraphDatabase) ODatabaseRecordThreadLocal.INSTANCE.get();

    final boolean safeMode = db.beginBlock();
    try {
      // OUT VERTEX
      db.acquireWriteLock(out);
      try {
        final String outFieldName = db.getOutVertexField(edge.getClassName());
        dropEdgeFromVertex(edge, outVertex, outFieldName, outVertex.field(outFieldName));
        db.save(out);
      } finally {
        db.releaseWriteLock(out);
      }

      // IN VERTEX
      final ODocument inVertex = edge.field(OPropertyGraph.EDGE_FIELD_IN);

      db.acquireWriteLock(in);
      try {
        final String inFieldName = db.getOutVertexField(edge.getClassName());
        dropEdgeFromVertex(edge, inVertex, inFieldName, inVertex.field(inFieldName));
        db.save(inVertex);
      } finally {
        db.releaseWriteLock(in);
      }

      db.delete(edge);

      db.commitBlock(safeMode);

    } catch (RuntimeException e) {
      db.rollbackBlock(safeMode);
      throw e;
    }
    return true;
  }
}
