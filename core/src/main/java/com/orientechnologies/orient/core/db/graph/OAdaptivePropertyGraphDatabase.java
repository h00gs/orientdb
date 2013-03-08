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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

/**
 * Adaptive implementation with these additional feature against OGraphDatabase class:
 * <ul>
 * <li>lazy creates edges only when needed</li>
 * <li>bind edges in different properties based on labels</li>
 * </ul>
 * 
 * @author Luca Garulli
 * 
 */
public class OAdaptivePropertyGraphDatabase extends OAbstractPropertyGraph {
  public static final String CONNECTION_OUT = "out";
  public static final String CONNECTION_IN  = "in";

  public OAdaptivePropertyGraphDatabase(final String iURL) {
    super(iURL);
  }

  public OAdaptivePropertyGraphDatabase(final ODatabaseRecordTx iSource) {
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

      // LOCK RESOURCES
      acquireWriteLock(iOutVertex);
      try {
        acquireWriteLock(iInVertex);
        try {

          if (iFields == null || iFields.length == 0) {
            // DON'T CREATE THE EDGE DOCUMENT, JUST THE LINKS
            createLink(iOutVertex, iInVertex, OPropertyGraph.VERTEX_FIELD_OUT + iClassName);
            createLink(iInVertex, iOutVertex, OPropertyGraph.VERTEX_FIELD_IN + iClassName);
            return null;
          } else {
            // CREATE THE EDGE DOCUMENT TO STORE FIELDS TOO
            final ODocument edge = new ODocument(cls).setOrdered(true);
            edge.field(OPropertyGraph.EDGE_FIELD_OUT, iOutVertex);
            edge.field(OPropertyGraph.EDGE_FIELD_IN, iInVertex);

            if (iFields != null)
              if (iFields.length == 1) {
                Object f = iFields[0];
                if (f instanceof Map<?, ?>)
                  edge.fields((Map<String, Object>) f);
                else
                  throw new IllegalArgumentException(
                      "Invalid fields: expecting a pairs of fields as String,Object or a single Map<String,Object>, but found: "
                          + f);
              } else
                // SET THE FIELDS
                for (int i = 0; i < iFields.length; i += 2)
                  edge.field(iFields[i].toString(), iFields[i + 1]);

            // OUT FIELD
            final Object outField = iOutVertex.field(OPropertyGraph.VERTEX_FIELD_OUT);
            if (outField == null)
              createLink(iOutVertex, edge, OPropertyGraph.VERTEX_FIELD_OUT + iClassName);
            else if (outField instanceof OMVRBTreeRIDSet) {
              OMVRBTreeRIDSet out = (OMVRBTreeRIDSet) outField;
              out.add(edge);
            } else
              throw new IllegalStateException("Relationship content is invalid. Found: " + outField);

            // IN FIELD
            final Object inField = iOutVertex.field(OPropertyGraph.VERTEX_FIELD_IN);
            if (inField == null)
              createLink(iInVertex, edge, OPropertyGraph.VERTEX_FIELD_IN + iClassName);
            else if (inField instanceof OMVRBTreeRIDSet) {
              OMVRBTreeRIDSet in = (OMVRBTreeRIDSet) inField;
              in.add(edge);
            } else
              throw new IllegalStateException("Relationship content is invalid. Found: " + inField);

            edge.setDirty();

            if (safeMode)
              save(edge);

            return edge;
          }
        } finally {
          releaseWriteLock(iInVertex);
        }
      } finally {
        releaseWriteLock(iOutVertex);
      }
    } catch (RuntimeException e) {
      rollbackBlock(safeMode);
      throw e;

    } finally {
      commitBlock(safeMode);
    }
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

      final OMVRBTreeRIDSet set = vertex.field(OPropertyGraph.VERTEX_FIELD_OUT);

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

      final OMVRBTreeRIDSet set = vertex.field(OPropertyGraph.VERTEX_FIELD_IN);

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
    OIdentifiable v = e.field(OPropertyGraph.EDGE_FIELD_IN);
    if (v != null && v instanceof ORID) {
      // REPLACE WITH THE DOCUMENT
      v = v.getRecord();
      final boolean wasDirty = e.isDirty();
      e.field(OPropertyGraph.EDGE_FIELD_IN, v);
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
    OIdentifiable v = e.field(OPropertyGraph.EDGE_FIELD_OUT);
    if (v != null && v instanceof ORID) {
      // REPLACE WITH THE DOCUMENT
      v = v.getRecord();
      final boolean wasDirty = e.isDirty();
      e.field(OPropertyGraph.EDGE_FIELD_OUT, v);
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

  public OClass checkEdgeClass(final String iEdgeTypeName) {
    if (iEdgeTypeName == null || !useCustomTypes)
      return getEdgeBaseClass();

    final OClass cls = getMetadata().getSchema().getClass(iEdgeTypeName);
    if (cls == null)
      throw new IllegalArgumentException("The class '" + iEdgeTypeName + "' was not found");

    if (!cls.isSubClassOf(edgeBaseClass))
      throw new IllegalArgumentException("The class '" + iEdgeTypeName + "' does not extend the edge type");

    return cls;
  }

  public void checkEdgeClass(final OClass iEdgeType) {
    if (useCustomTypes && iEdgeType != null) {
      if (!iEdgeType.isSubClassOf(edgeBaseClass))
        throw new IllegalArgumentException("The class '" + iEdgeType + "' does not extend the edge type");
    }
  }

  public boolean isUseCustomTypes() {
    return useCustomTypes;
  }

  public void setUseCustomTypes(boolean useCustomTypes) {
    this.useCustomTypes = useCustomTypes;
  }

  /**
   * Returns true if the document is a vertex (its class is OGraphVertex or any subclasses)
   * 
   * @param iRecord
   *          Document to analyze.
   * @return true if the document is a vertex (its class is OGraphVertex or any subclasses)
   */
  public boolean isVertex(final ODocument iRecord) {
    return iRecord != null ? iRecord.getSchemaClass().isSubClassOf(vertexBaseClass) : false;
  }

  /**
   * Returns true if the document is an edge (its class is OGraphEdge or any subclasses)
   * 
   * @param iRecord
   *          Document to analyze.
   * @return true if the document is a edge (its class is OGraphEdge or any subclasses)
   */
  public boolean isEdge(final ODocument iRecord) {
    return iRecord != null ? iRecord.getSchemaClass().isSubClassOf(edgeBaseClass) : false;
  }

  /**
   * Locks the record in exclusive mode to avoid concurrent access.
   * 
   * @param iRecord
   *          Record to lock
   * @return The current instance as fluent interface to allow calls in chain.
   */
  public OAdaptivePropertyGraphDatabase acquireWriteLock(final OIdentifiable iRecord) {
    switch (lockMode) {
    case DATABASE_LEVEL_LOCKING:
      ((OStorage) getStorage()).getLock().acquireExclusiveLock();
      break;
    case RECORD_LEVEL_LOCKING:
      ((OStorageEmbedded) getStorage()).acquireWriteLock(iRecord.getIdentity());
      break;
    case NO_LOCKING:
      break;
    }
    return this;
  }

  /**
   * Releases the exclusive lock against a record previously acquired by current thread.
   * 
   * @param iRecord
   *          Record to unlock
   * @return The current instance as fluent interface to allow calls in chain.
   */
  public OAdaptivePropertyGraphDatabase releaseWriteLock(final OIdentifiable iRecord) {
    switch (lockMode) {
    case DATABASE_LEVEL_LOCKING:
      ((OStorage) getStorage()).getLock().releaseExclusiveLock();
      break;
    case RECORD_LEVEL_LOCKING:
      ((OStorageEmbedded) getStorage()).releaseWriteLock(iRecord.getIdentity());
      break;
    case NO_LOCKING:
      break;
    }
    return this;
  }

  /**
   * Locks the record in shared mode to avoid concurrent writes.
   * 
   * @param iRecord
   *          Record to lock
   * @return The current instance as fluent interface to allow calls in chain.
   */
  public OAdaptivePropertyGraphDatabase acquireReadLock(final OIdentifiable iRecord) {
    switch (lockMode) {
    case DATABASE_LEVEL_LOCKING:
      ((OStorage) getStorage()).getLock().acquireSharedLock();
      break;
    case RECORD_LEVEL_LOCKING:
      ((OStorageEmbedded) getStorage()).acquireReadLock(iRecord.getIdentity());
      break;
    case NO_LOCKING:
      break;
    }
    return this;
  }

  /**
   * Releases the shared lock against a record previously acquired by current thread.
   * 
   * @param iRecord
   *          Record to unlock
   * @return The current instance as fluent interface to allow calls in chain.
   */
  public OAdaptivePropertyGraphDatabase releaseReadLock(final OIdentifiable iRecord) {
    switch (lockMode) {
    case DATABASE_LEVEL_LOCKING:
      ((OStorage) getStorage()).getLock().releaseSharedLock();
      break;
    case RECORD_LEVEL_LOCKING:
      ((OStorageEmbedded) getStorage()).releaseReadLock(iRecord.getIdentity());
      break;
    case NO_LOCKING:
      break;
    }
    return this;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  public void checkForGraphSchema() {
    getMetadata().getSchema().getOrCreateClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);

    vertexBaseClass = getMetadata().getSchema().getClass(OPropertyGraph.VERTEX_CLASS_NAME);
    edgeBaseClass = getMetadata().getSchema().getClass(OPropertyGraph.EDGE_CLASS_NAME);

    if (vertexBaseClass == null) {
      // CREATE THE META MODEL USING THE ORIENT SCHEMA
      vertexBaseClass = getMetadata().getSchema().createClass(OPropertyGraph.VERTEX_CLASS_NAME);
      vertexBaseClass.setShortName("V");
      vertexBaseClass.setOverSize(2);

      if (edgeBaseClass == null) {
        edgeBaseClass = getMetadata().getSchema().createClass(OPropertyGraph.EDGE_CLASS_NAME);
        edgeBaseClass.setShortName("E");
      }

      vertexBaseClass.createProperty(OPropertyGraph.VERTEX_FIELD_IN, OType.LINKSET, edgeBaseClass);
      vertexBaseClass.createProperty(OPropertyGraph.VERTEX_FIELD_OUT, OType.LINKSET, edgeBaseClass);
      edgeBaseClass.createProperty(OPropertyGraph.EDGE_FIELD_IN, OType.LINK, vertexBaseClass);
      edgeBaseClass.createProperty(OPropertyGraph.EDGE_FIELD_OUT, OType.LINK, vertexBaseClass);
    }
  }

  protected boolean checkEdge(final ODocument iEdge, final String[] iLabels, final String[] iClassNames) {
    boolean good = true;

    if (iClassNames != null) {
      // CHECK AGAINST CLASS NAMES
      good = false;
      for (String c : iClassNames) {
        if (c.equals(iEdge.getClassName())) {
          good = true;
          break;
        }
      }
    }

    if (good && iLabels != null) {
      // CHECK AGAINST LABELS
      good = false;
      for (String c : iLabels) {
        if (c.equals(iEdge.field(OPropertyGraph.LABEL))) {
          good = true;
          break;
        }
      }
    }
    return good;
  }

  public LOCK_MODE getLockMode() {
    return lockMode;
  }

  public void setLockMode(final LOCK_MODE lockMode) {
    if (lockMode == LOCK_MODE.RECORD_LEVEL_LOCKING && !(getStorage() instanceof OStorageEmbedded))
      // NOT YET SUPPORETD REMOTE LOCKING
      throw new IllegalArgumentException("Record leve locking is not supported for remote connections");

    this.lockMode = lockMode;
  }

  public static DIRECTION getConnectionDirection(final String iConnectionField) {
    if (iConnectionField == null)
      throw new IllegalArgumentException("Cannot return direction of NULL connection ");

    if (iConnectionField.startsWith(CONNECTION_OUT))
      return DIRECTION.OUT;
    else if (iConnectionField.startsWith(CONNECTION_IN))
      return DIRECTION.IN;

    throw new IllegalArgumentException("Cannot return direction of connection " + iConnectionField);
  }

  public static DIRECTION getOppositeDirection(final DIRECTION iDirection) {
    if (iDirection == null || iDirection == DIRECTION.BOTH)
      throw new IllegalArgumentException("Cannot return opposite direction of " + iDirection);

    return iDirection == DIRECTION.OUT ? DIRECTION.IN : DIRECTION.OUT;
  }

  public static String getInverseConnectionFieldName(final String iFieldName) {
    if (iFieldName.startsWith(CONNECTION_OUT)) {
      if (iFieldName.length() == CONNECTION_OUT.length())
        // "OUT" CASE
        return CONNECTION_IN;

      return CONNECTION_IN + iFieldName.substring(CONNECTION_OUT.length());

    } else if (iFieldName.startsWith(CONNECTION_IN)) {
      if (iFieldName.length() == CONNECTION_IN.length())
        // "IN" CASE
        return CONNECTION_OUT;

      return CONNECTION_OUT + iFieldName.substring(CONNECTION_IN.length());

    } else
      throw new IllegalArgumentException("Cannot find reverse connection name for field " + iFieldName);
  }

  protected Object createLink(final ODocument iFromVertex, final ODocument iToVertex, final String iFieldName) {
    final Object out;
    Object found = iFromVertex.field(iFieldName);
    if (found == null)
      // CREATE ONLY ONE LINK
      out = iToVertex;
    else if (found instanceof OIdentifiable) {
      // DOUBLE: SCALE UP THE LINK INTO A COLLECTION
      out = new OMVRBTreeRIDSet(iFromVertex);
      ((OMVRBTreeRIDSet) out).add((OIdentifiable) found);
    } else if (found instanceof OMVRBTreeRIDSet) {
      // ADD THE LINK TO THE COLLECTION
      out = found;
      ((OMVRBTreeRIDSet) out).add((OIdentifiable) found);
    } else
      throw new IllegalStateException("Relationship content is invalid. Found: " + found);

    iFromVertex.field(iFieldName, out);
    return out;
  }
}
