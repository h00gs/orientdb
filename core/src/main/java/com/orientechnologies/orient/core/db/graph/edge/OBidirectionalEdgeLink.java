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
import com.orientechnologies.orient.core.db.graph.OLabeledGraph.DIRECTION;
import com.orientechnologies.orient.core.db.graph.OPropertyGraphDatabase;
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
public class OBidirectionalEdgeLink extends OAbstractEdge {
  protected final OIdentifiable out;
  protected final OIdentifiable in;
  protected final String        edgeClassName;

  public OBidirectionalEdgeLink(final OIdentifiable iOut, final OIdentifiable iIn, final String iEdgeClassName) {
    this.out = iOut;
    this.in = iIn;
    this.edgeClassName = iEdgeClassName;
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
    return edgeClassName;
  }

  public boolean drop() {
    final ODocument outVertex = out.getRecord();
    if (outVertex == null)
      return false;

    final ODocument inVertex = in.getRecord();
    if (inVertex == null)
      return false;

    final OPropertyGraphDatabase db = (OPropertyGraphDatabase) ODatabaseRecordThreadLocal.INSTANCE.get();

    final boolean safeMode = db.beginBlock();
    try {
      // OUT VERTEX
      db.acquireWriteLock(outVertex);
      try {
        final String outFieldName = OVertexDocument.getConnectionFieldName(DIRECTION.OUT, edgeClassName);
        dropEdgeFromVertex(inVertex, outVertex, outFieldName, outVertex.field(outFieldName));
        db.save(outVertex);
      } finally {
        db.releaseWriteLock(outVertex);
      }

      // IN VERTEX
      db.acquireWriteLock(inVertex);
      try {
        final String inFieldName = OVertexDocument.getConnectionFieldName(DIRECTION.IN, edgeClassName);
        dropEdgeFromVertex(outVertex, inVertex, inFieldName, inVertex.field(inFieldName));
        db.save(inVertex);
      } finally {
        db.releaseWriteLock(inVertex);
      }

      db.commitBlock(safeMode);

    } catch (RuntimeException e) {
      db.rollbackBlock(safeMode);
      throw e;
    }
    return true;
  }
}
