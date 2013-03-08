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
package com.orientechnologies.orient.core.db.graph.vertex;

import java.util.Collection;
import java.util.Iterator;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.graph.OAdaptivePropertyGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OLabeledGraph.DIRECTION;
import com.orientechnologies.orient.core.db.graph.edge.OBidirectionalEdgeDocument;
import com.orientechnologies.orient.core.db.graph.edge.OBidirectionalEdgeLink;
import com.orientechnologies.orient.core.db.graph.edge.OEdge;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.iterator.OLazyWrapperIterator;
import com.orientechnologies.orient.core.iterator.OMultiCollectionIterator;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

public class OVertexDocument implements OVertex {
  public static final String VERTEX_CLASS_NAME = "OGraphVertex";
  public static final String VERTEX_FIELD_OUT  = OAdaptivePropertyGraphDatabase.CONNECTION_OUT;
  public static final String VERTEX_FIELD_IN   = OAdaptivePropertyGraphDatabase.CONNECTION_IN;

  protected OIdentifiable    record;

  public OVertexDocument() {
    record = new ODocument(VERTEX_CLASS_NAME);
  }

  public OVertexDocument(final OIdentifiable iRecord) {
    record = iRecord;
  }

  @Override
  public Iterator<OVertex> getOutVertices() {
    return getVertices(DIRECTION.OUT, null);
  }

  @Override
  public Iterator<OVertex> getOutVertices(final String iClassNames) {
    return getVertices(DIRECTION.OUT, iClassNames);
  }

  @Override
  public Iterator<OVertex> getInVertices() {
    return getVertices(DIRECTION.IN, null);
  }

  @Override
  public Iterator<OVertex> getInVertices(final String iClassNames) {
    return getVertices(DIRECTION.IN, iClassNames);
  }

  @Override
  public boolean drop() {
    final ODocument doc = getDocument();

    final OAdaptivePropertyGraphDatabase db = (OAdaptivePropertyGraphDatabase) ODatabaseRecordThreadLocal.INSTANCE.get();

    final boolean safeMode = db.beginBlock();
    try {
      db.acquireWriteLock(doc);
      try {
        for (String fieldName : doc.fieldNames()) {
          final DIRECTION direction = getFieldDirection(DIRECTION.BOTH, fieldName, null);
          if (direction == null)
            // SKIP THIS FIELD
            continue;

          removeEdges(doc, fieldName, null);
        }

        doc.delete();

      } finally {
        db.releaseWriteLock(doc);
      }
      db.commitBlock(safeMode);

    } catch (RuntimeException e) {
      db.rollbackBlock(safeMode);
      throw e;
    }

    return true;
  }

  /**
   * Returns connected vertices. OVertex objects are created lazy at browsing time.
   * 
   * @param iDirection
   *          direction between OUT, IN or BOTH
   * @param iClassName
   *          Class name to filter. If null the class name is ignored
   */
  public Iterator<OVertex> getVertices(final DIRECTION iDirection, final String iClassName) {
    return new OLazyWrapperIterator<OVertex>(getVerticesAsRecords(iDirection, iClassName)) {
      @Override
      public OVertex createWrapper(final Object iObject) {
        return iObject instanceof OVertex ? (OVertex) iObject : new OVertexDocument((OIdentifiable) iObject);
      }
    };
  }

  /**
   * Returns connected vertices as records.
   * 
   * @param iDirection
   *          direction between OUT, IN or BOTH
   * @param iClassName
   *          Class name to filter. If null the class name is ignored
   */
  public Iterator<OIdentifiable> getVerticesAsRecords(final DIRECTION iDirection, final String iClassName) {
    final ODocument doc = getDocument();

    final OMultiCollectionIterator<OIdentifiable> iterator = new OMultiCollectionIterator<OIdentifiable>();
    for (String fieldName : doc.fieldNames()) {
      final DIRECTION direction = getFieldDirection(iDirection, fieldName, iClassName);
      if (direction == null)
        // SKIP THIS FIELD
        continue;

      final Object fieldValue = doc.field(fieldName);
      if (fieldValue != null)
        if (fieldValue instanceof OIdentifiable) {
          final ODocument fieldRecord = ((OIdentifiable) fieldValue).getRecord();
          if (fieldRecord.getSchemaClass().isSubClassOf(VERTEX_CLASS_NAME))
            // DIRECT VERTEX
            iterator.add(fieldValue);
          else if (fieldRecord.getSchemaClass().isSubClassOf(OBidirectionalEdgeDocument.EDGE_CLASS_NAME)) {
            // EDGE
            final OIdentifiable otherVertex = OBidirectionalEdgeDocument.getVertex(fieldRecord,
                direction == DIRECTION.OUT ? DIRECTION.IN : DIRECTION.OUT);
            iterator.add(otherVertex);
          } else
            throw new IllegalStateException("Invalid content found in " + fieldName + " field");

        } else if (fieldValue instanceof Collection<?>) {
          iterator.add(fieldValue);
        }
    }

    return iterator;
  }

  @Override
  public Iterator<OEdge> getOutEdges() {
    return getEdges(DIRECTION.OUT, null);
  }

  @Override
  public Iterator<OEdge> getOutEdges(final String iClassNames) {
    return getEdges(DIRECTION.OUT, iClassNames);
  }

  @Override
  public Iterator<OEdge> getInEdges() {
    return getEdges(DIRECTION.IN, null);
  }

  @Override
  public Iterator<OEdge> getInEdges(final String iClassNames) {
    return getEdges(DIRECTION.IN, iClassNames);
  }

  /**
   * Returns connected edges.
   * 
   * @param iDirection
   *          direction between OUT, IN or BOTH
   * @param iClassName
   *          Class name to filter. If null the class name is ignored
   */
  public Iterator<OEdge> getEdges(final DIRECTION iDirection, final String iClassName) {
    final ODocument doc = getDocument();

    final OMultiCollectionIterator<OEdge> iterator = new OMultiCollectionIterator<OEdge>();
    for (String fieldName : doc.fieldNames()) {
      final DIRECTION direction = getFieldDirection(iDirection, fieldName, iClassName);
      if (direction == null)
        // SKIP THIS FIELD
        continue;

      final Object fieldValue = doc.field(fieldName);
      if (fieldValue != null)
        if (fieldValue instanceof OIdentifiable) {
          final ODocument fieldRecord = ((OIdentifiable) fieldValue).getRecord();
          if (fieldRecord.getSchemaClass().isSubClassOf(VERTEX_CLASS_NAME))
            // DIRECT VERTEX
            iterator.add(new OBidirectionalEdgeLink(doc, fieldRecord, fieldName));
          else if (fieldRecord.getSchemaClass().isSubClassOf(OBidirectionalEdgeDocument.EDGE_CLASS_NAME)) {
            // EDGE
            iterator.add(new OBidirectionalEdgeDocument(fieldRecord));
          } else
            throw new IllegalStateException("Invalid content found in " + fieldName + " field");

        } else if (fieldValue instanceof Collection<?>) {
          // CREATE LAZY ITERATOR AGAINST COLLECTION FIELD
          iterator.add(new OLazyWrapperIterator<OEdge>(((Collection<?>) fieldValue).iterator(), fieldName) {
            @Override
            public OEdge createWrapper(final Object iObject) {
              if (iObject instanceof OEdge)
                return (OEdge) iObject;

              final ODocument value = ((OIdentifiable) iObject).getRecord();
              if (value.getSchemaClass().isSubClassOf(VERTEX_CLASS_NAME))
                // DIRECT VERTEX
                return new OBidirectionalEdgeLink(doc, value, (String) additionalData);
              else if (value.getSchemaClass().isSubClassOf(OBidirectionalEdgeDocument.EDGE_CLASS_NAME)) {
                // EDGE
                return new OBidirectionalEdgeDocument(value);
              } else
                throw new IllegalStateException("Invalid content found between connections:" + value);
            }
          });
        }
    }

    return iterator;
  }

  public static Object getConnections(final ODocument iVertex, final DIRECTION iDirection, final String iClassName) {
    return iVertex.field(iDirection == DIRECTION.OUT ? VERTEX_FIELD_OUT : VERTEX_FIELD_IN + iClassName);
  }

  protected ODocument getDocument() {
    final ODocument doc = record.getRecord();
    if (doc == null)
      throw new IllegalStateException("Vertex " + record + " is deleted");
    return doc;
  }

  protected DIRECTION getFieldDirection(final DIRECTION iDirection, String fieldName, final String iClassName) {
    DIRECTION direction = null;

    if ((iDirection == DIRECTION.OUT || iDirection == DIRECTION.BOTH) && fieldName.startsWith(VERTEX_FIELD_OUT)) {
      if (iClassName == null || fieldName.equals(VERTEX_FIELD_OUT + iClassName))
        direction = DIRECTION.OUT;
    } else if ((iDirection == DIRECTION.IN || iDirection == DIRECTION.BOTH) && fieldName.startsWith(VERTEX_FIELD_IN)) {
      if (iClassName == null || fieldName.equals(VERTEX_FIELD_IN + iClassName))
        direction = DIRECTION.IN;
    }

    return direction;
  }

  protected boolean removeEdges(final ODocument iVertex, final String iFieldName, final OIdentifiable iVertexToRemove) {
    if (iVertex == null)
      return false;

    final Object fieldValue = iVertexToRemove == null ? iVertex.field(iFieldName) : iVertex.removeField(iFieldName);
    if (fieldValue == null)
      return false;

    if (fieldValue instanceof OIdentifiable) {
      // SINGLE RECORD

      if (iVertexToRemove != null && !fieldValue.equals(iVertexToRemove)) {
        OLogManager.instance().warn(this, "[OVertexDocument.removeEdge] connections %s not found in field %s", iVertexToRemove,
            iFieldName);
        return false;
      }

      removeEdge(iVertex, iFieldName, iVertexToRemove, fieldValue);

    } else if (fieldValue instanceof OMVRBTreeRIDSet) {
      // COLLECTION OF RECORDS: REMOVE THE ENTRY

      if (iVertexToRemove != null)
        for (OLazyIterator<OIdentifiable> it = ((OMVRBTreeRIDSet) fieldValue).iterator(false); it.hasNext();) {
          final ODocument curr = it.next().getRecord();

          if (iVertexToRemove.equals(curr))
            it.remove();

          removeEdge(iVertex, iFieldName, iVertexToRemove, curr);
        }
    }

    return true;
  }

  private boolean removeEdge(final ODocument iVertex, final String iFieldName, final OIdentifiable iVertexToRemove,
      final Object iFieldValue) {
    final String inverseFieldName = OAdaptivePropertyGraphDatabase.getInverseConnectionFieldName(iFieldName);

    final ODocument r = ((OIdentifiable) iFieldValue).getRecord();
    if (r.getSchemaClass().isSubClassOf(VERTEX_CLASS_NAME)) {
      // DIRECT VERTEX
      return removeEdges(r, inverseFieldName, iVertex);

    } else if (r.getSchemaClass().isSubClassOf(OBidirectionalEdgeDocument.EDGE_CLASS_NAME)) {
      // EDGE, REMOVE THE EDGE
      final OIdentifiable otherVertex = OBidirectionalEdgeDocument.getVertex(r,
          OAdaptivePropertyGraphDatabase.getConnectionDirection(inverseFieldName));

      if (otherVertex != null) {
        if (iVertexToRemove != null && !otherVertex.equals(iVertexToRemove)) {
          OLogManager.instance().warn(this, "[OVertexDocument.removeEdge] connections %s not found in field %s", iVertexToRemove,
              iFieldName);
          return false;
        } else
          // BIDIRECTIONAL EDGE
          return removeEdges((ODocument) otherVertex.getRecord(), inverseFieldName, iVertex);
      }

      return false;

    } else
      throw new IllegalStateException("Invalid content found in " + iFieldName + " field");
  }
}
