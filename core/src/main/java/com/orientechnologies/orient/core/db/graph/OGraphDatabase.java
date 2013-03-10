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
package com.orientechnologies.orient.core.db.graph;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.db.graph.edge.OBidirectionalEdgeDocument;
import com.orientechnologies.orient.core.db.graph.vertex.OVertexDocument;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Super light GraphDB implementation on top of the underlying Document. The generated vertexes and edges are compatible with those
 * of ODatabaseGraphTx and TinkerPop Blueprints implementation. This class is the fastest and lightest but you have ODocument
 * instances and not regular ad-hoc POJO as for other implementations. You could use this one for bulk operations and the others for
 * regular graph access.
 * 
 * @author Luca Garulli
 * 
 */
public class OGraphDatabase extends OAbstractPropertyGraph {
  public OGraphDatabase(final String iURL) {
    super(iURL);
  }

  public OGraphDatabase(final ODatabaseRecordTx iSource) {
    super(iSource);
    checkForGraphSchema();
  }

  @SuppressWarnings("unchecked")
  public ODocument createEdge(final ODocument iOutVertex, final ODocument iInVertex, final String iClassName, Object... iFields) {
    if (iOutVertex == null)
      throw new IllegalArgumentException("iOutVertex is null");

    if (iInVertex == null)
      throw new IllegalArgumentException("iInVertex is null");

    final OClass cls = checkEdgeClass(iClassName);

    final boolean safeMode = beginBlock();
    try {

      final ODocument edge = new ODocument(cls).setOrdered(true);
      edge.field(OBidirectionalEdgeDocument.EDGE_FIELD_OUT, iOutVertex);
      edge.field(OBidirectionalEdgeDocument.EDGE_FIELD_IN, iInVertex);

      if (iFields != null)
        if (iFields.length == 1) {
          Object f = iFields[0];
          if (f instanceof Map<?, ?>)
            edge.fields((Map<String, Object>) f);
          else
            throw new IllegalArgumentException(
                "Invalid fields: expecting a pairs of fields as String,Object or a single Map<String,Object>, but found: " + f);
        } else
          // SET THE FIELDS
          for (int i = 0; i < iFields.length; i += 2)
            edge.field(iFields[i].toString(), iFields[i + 1]);

      // OUT FIELD
      acquireWriteLock(iOutVertex);
      try {

        final Object outField = iOutVertex.field(OVertexDocument.VERTEX_FIELD_OUT);
        final OMVRBTreeRIDSet out;
        if (outField instanceof OMVRBTreeRIDSet) {
          out = (OMVRBTreeRIDSet) outField;
        } else if (outField instanceof Collection<?>) {
          out = new OMVRBTreeRIDSet(iOutVertex, (Collection<OIdentifiable>) outField);
          iOutVertex.field(OVertexDocument.VERTEX_FIELD_OUT, out);
        } else {
          out = new OMVRBTreeRIDSet(iOutVertex);
          iOutVertex.field(OVertexDocument.VERTEX_FIELD_OUT, out);
        }
        out.add(edge);
      } finally {
        releaseWriteLock(iOutVertex);
      }

      // IN FIELD
      acquireWriteLock(iInVertex);
      try {

        final Object inField = iInVertex.field(OVertexDocument.VERTEX_FIELD_IN);
        final OMVRBTreeRIDSet in;
        if (inField instanceof OMVRBTreeRIDSet) {
          in = (OMVRBTreeRIDSet) inField;
        } else if (inField instanceof Collection<?>) {
          in = new OMVRBTreeRIDSet(iInVertex, (Collection<OIdentifiable>) inField);
          iInVertex.field(OVertexDocument.VERTEX_FIELD_IN, in);
        } else {
          in = new OMVRBTreeRIDSet(iInVertex);
          iInVertex.field(OVertexDocument.VERTEX_FIELD_IN, in);
        }
        in.add(edge);

      } finally {
        releaseWriteLock(iInVertex);
      }

      edge.setDirty();

      if (safeMode) {
        save(edge);
        commitBlock(safeMode);
      }

      return edge;

    } catch (RuntimeException e) {
      rollbackBlock(safeMode);
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  public boolean removeEdge(final OIdentifiable iEdge) {
    if (iEdge == null)
      return false;

    final ODocument edge = iEdge.getRecord();
    if (edge == null)
      return false;

    final boolean safeMode = beginBlock();
    try {
      // OUT VERTEX
      final ODocument outVertex = edge.field(OBidirectionalEdgeDocument.EDGE_FIELD_OUT);

      acquireWriteLock(outVertex);
      try {

        if (outVertex != null) {
          final Set<ODocument> out = ((Set<ODocument>) outVertex.field(OVertexDocument.VERTEX_FIELD_OUT));
          if (out != null)
            out.remove(edge);
          save(outVertex);
        }

      } finally {
        releaseWriteLock(outVertex);
      }

      // IN VERTEX
      final ODocument inVertex = edge.field(OBidirectionalEdgeDocument.EDGE_FIELD_IN);

      acquireWriteLock(inVertex);
      try {

        if (inVertex != null) {
          final Set<ODocument> in = ((Set<ODocument>) inVertex.field(OVertexDocument.VERTEX_FIELD_IN));
          if (in != null)
            in.remove(edge);
          save(inVertex);
        }

      } finally {
        releaseWriteLock(inVertex);
      }

      delete(edge);

      commitBlock(safeMode);

    } catch (RuntimeException e) {
      rollbackBlock(safeMode);
      throw e;
    }
    return true;
  }

  /**
   * Returns all the edges between the vertexes iVertex1 and iVertex2 with label between the array of labels passed as iLabels and
   * with class between the array of class names passed as iClassNames.
   * 
   * @param iVertex1
   *          First Vertex
   * @param iVertex2
   *          Second Vertex
   * @param iLabels
   *          Array of strings with the labels to get as filter
   * @param iClassNames
   *          Array of strings with the name of the classes to get as filter
   * @return The Set with the common Edges between the two vertexes. If edges aren't found the set is empty
   */
  public Set<OIdentifiable> getEdgesBetweenVertexes(final OIdentifiable iVertex1, final OIdentifiable iVertex2,
      final String[] iLabels, final String[] iClassNames) {
    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    if (iVertex1 != null && iVertex2 != null) {
      acquireReadLock(iVertex1);
      try {

        // CHECK OUT EDGES
        for (OIdentifiable e : getOutEdges(iVertex1)) {
          final ODocument edge = (ODocument) e.getRecord();

          if (checkEdge(edge, iLabels, iClassNames)) {
            final OIdentifiable in = edge.<ODocument> field("in");
            if (in != null && in.equals(iVertex2))
              result.add(edge);
          }
        }

        // CHECK IN EDGES
        for (OIdentifiable e : getInEdges(iVertex1)) {
          final ODocument edge = (ODocument) e.getRecord();

          if (checkEdge(edge, iLabels, iClassNames)) {
            final OIdentifiable out = edge.<ODocument> field("out");
            if (out != null && out.equals(iVertex2))
              result.add(edge);
          }
        }

      } finally {
        releaseReadLock(iVertex1);
      }

    }

    return result;
  }

  /**
   * Retrieves the outgoing edges of vertex iVertex having label equals to iLabel.
   * 
   * @param iVertex
   *          Target vertex
   * @param iLabel
   *          Label to search
   * @return
   */
  public Set<OIdentifiable> getOutEdges(final OIdentifiable iVertex, final String iLabel) {
    if (iVertex == null)
      return null;

    final ODocument vertex = iVertex.getRecord();
    checkVertexClass(vertex);

    Set<OIdentifiable> result = null;

    acquireReadLock(iVertex);
    try {

      final OMVRBTreeRIDSet set = vertex.field(OVertexDocument.VERTEX_FIELD_OUT);

      if (iLabel == null)
        // RETURN THE ENTIRE COLLECTION
        if (set != null)
          return Collections.unmodifiableSet(set);
        else
          return Collections.emptySet();

      // FILTER BY LABEL
      result = new HashSet<OIdentifiable>();
      if (set != null)
        for (OIdentifiable item : set) {
          if (iLabel == null || iLabel.equals(((ODocument) item).field(OPropertyGraph.LABEL)))
            result.add(item);
        }

    } finally {
      releaseReadLock(iVertex);
    }

    return result;
  }

  public Set<OIdentifiable> getInEdges(final OIdentifiable iVertex, final String iLabel) {
    if (iVertex == null)
      return null;

    final ODocument vertex = iVertex.getRecord();
    checkVertexClass(vertex);

    Set<OIdentifiable> result = null;

    acquireReadLock(iVertex);
    try {

      final OMVRBTreeRIDSet set = vertex.field(OVertexDocument.VERTEX_FIELD_IN);

      if (iLabel == null)
        // RETURN THE ENTIRE COLLECTION
        if (set != null)
          return Collections.unmodifiableSet(set);
        else
          return Collections.emptySet();

      // FILTER BY LABEL
      result = new HashSet<OIdentifiable>();
      if (set != null)
        for (OIdentifiable item : set) {
          if (iLabel == null || iLabel.equals(((ODocument) item).field(OPropertyGraph.LABEL)))
            result.add(item);
        }

    } finally {
      releaseReadLock(iVertex);
    }
    return result;
  }

  public ODocument getInVertex(final OIdentifiable iEdge) {
    if (iEdge == null)
      return null;

    final ODocument e = (ODocument) iEdge.getRecord();

    checkEdgeClass(e);
    OIdentifiable v = e.field(OBidirectionalEdgeDocument.EDGE_FIELD_IN);
    if (v != null && v instanceof ORID) {
      // REPLACE WITH THE DOCUMENT
      v = v.getRecord();
      final boolean wasDirty = e.isDirty();
      e.field(OBidirectionalEdgeDocument.EDGE_FIELD_IN, v);
      if (!wasDirty)
        e.unsetDirty();
    }

    return (ODocument) v;
  }

  public ODocument getOutVertex(final OIdentifiable iEdge) {
    if (iEdge == null)
      return null;

    final ODocument e = (ODocument) iEdge.getRecord();

    checkEdgeClass(e);
    OIdentifiable v = e.field(OBidirectionalEdgeDocument.EDGE_FIELD_OUT);
    if (v != null && v instanceof ORID) {
      // REPLACE WITH THE DOCUMENT
      v = v.getRecord();
      final boolean wasDirty = e.isDirty();
      e.field(OBidirectionalEdgeDocument.EDGE_FIELD_OUT, v);
      if (!wasDirty)
        e.unsetDirty();
    }

    return (ODocument) v;
  }

  public Set<OIdentifiable> filterEdgesByProperties(final OMVRBTreeRIDSet iEdges, final Iterable<String> iPropertyNames) {
    acquireReadLock(null);
    try {

      if (iPropertyNames == null)
        // RETURN THE ENTIRE COLLECTION
        if (iEdges != null)
          return Collections.unmodifiableSet(iEdges);
        else
          return Collections.emptySet();

      // FILTER BY PROPERTY VALUES
      final OMVRBTreeRIDSet result = new OMVRBTreeRIDSet();
      if (iEdges != null)
        for (OIdentifiable item : iEdges) {
          final ODocument doc = (ODocument) item;
          for (String propName : iPropertyNames) {
            if (doc.containsField(propName))
              // FOUND: ADD IT
              result.add(item);
          }
        }

      return result;

    } finally {
      releaseReadLock(null);
    }
  }

  public Set<OIdentifiable> filterEdgesByProperties(final OMVRBTreeRIDSet iEdges, final Map<String, Object> iProperties) {
    acquireReadLock(null);
    try {

      if (iProperties == null)
        // RETURN THE ENTIRE COLLECTION
        if (iEdges != null)
          return Collections.unmodifiableSet(iEdges);
        else
          return Collections.emptySet();

      // FILTER BY PROPERTY VALUES
      final OMVRBTreeRIDSet result = new OMVRBTreeRIDSet();
      if (iEdges != null)
        for (OIdentifiable item : iEdges) {
          final ODocument doc = (ODocument) item;
          for (Entry<String, Object> prop : iProperties.entrySet()) {
            if (prop.getKey() != null && doc.containsField(prop.getKey())) {
              if (prop.getValue() == null) {
                if (doc.field(prop.getKey()) == null)
                  // BOTH NULL: ADD IT
                  result.add(item);
              } else if (prop.getValue().equals(doc.field(prop.getKey())))
                // SAME VALUE: ADD IT
                result.add(item);
            }
          }
        }

      return result;
    } finally {
      releaseReadLock(null);
    }
  }

  @Override
  public void checkForGraphSchema() {
    super.checkForGraphSchema();

    if (vertexBaseClass.getProperty(OVertexDocument.VERTEX_FIELD_IN) == null)
      vertexBaseClass.createProperty(OVertexDocument.VERTEX_FIELD_IN, OType.LINKSET, edgeBaseClass);
    if (vertexBaseClass.getProperty(OVertexDocument.VERTEX_FIELD_OUT) == null)
      vertexBaseClass.createProperty(OVertexDocument.VERTEX_FIELD_OUT, OType.LINKSET, edgeBaseClass);

    if (edgeBaseClass.getProperty(OBidirectionalEdgeDocument.EDGE_FIELD_IN) == null)
      edgeBaseClass.createProperty(OBidirectionalEdgeDocument.EDGE_FIELD_IN, OType.LINK, vertexBaseClass);
    if (edgeBaseClass.getProperty(OBidirectionalEdgeDocument.EDGE_FIELD_OUT) == null)
      edgeBaseClass.createProperty(OBidirectionalEdgeDocument.EDGE_FIELD_OUT, OType.LINK, vertexBaseClass);
  }
}
