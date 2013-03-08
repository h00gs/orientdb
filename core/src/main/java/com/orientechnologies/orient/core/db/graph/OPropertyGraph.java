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

import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

public interface OPropertyGraph extends ODatabase, OLabeledGraph {
  public enum LOCK_MODE {
    NO_LOCKING, DATABASE_LEVEL_LOCKING, RECORD_LEVEL_LOCKING
  }

  public static final String LABEL = "label";

  public long countEdges();

  public Iterable<ODocument> browseEdges();

  public Iterable<ODocument> browseEdges(boolean iPolymorphic);

  public ODocument createEdge(ODocument iOutVertex, ODocument iInVertex, String iClassName, Object... iFields);

  /**
   * Returns all the edges between the vertexes iVertex1 and iVertex2.
   * 
   * @param iVertex1
   *          First Vertex
   * @param iVertex2
   *          Second Vertex
   * @return The Set with the common Edges between the two vertexes. If edges aren't found the set is empty
   */
  public Set<OIdentifiable> getEdgesBetweenVertexes(OIdentifiable iVertex1, OIdentifiable iVertex2);

  /**
   * Returns all the edges between the vertexes iVertex1 and iVertex2 with label between the array of labels passed as iLabels.
   * 
   * @param iVertex1
   *          First Vertex
   * @param iVertex2
   *          Second Vertex
   * @param iLabels
   *          Array of strings with the labels to get as filter
   * @return The Set with the common Edges between the two vertexes. If edges aren't found the set is empty
   */
  public Set<OIdentifiable> getEdgesBetweenVertexes(OIdentifiable iVertex1, OIdentifiable iVertex2, String[] iLabels);

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
  public Set<OIdentifiable> getEdgesBetweenVertexes(OIdentifiable iVertex1, OIdentifiable iVertex2, String[] iLabels,
      String[] iClassNames);

  public Set<OIdentifiable> getOutEdges(OIdentifiable iVertex);

  /**
   * Retrieves the outgoing edges of vertex iVertex having label equals to iLabel.
   * 
   * @param iVertex
   *          Target vertex
   * @param iLabel
   *          Label to search
   * @return
   */
  public Set<OIdentifiable> getOutEdges(OIdentifiable iVertex, String iLabel);

  /**
   * Retrieves the outgoing edges of vertex iVertex having the requested properties iProperties set to the passed values
   * 
   * @param iVertex
   *          Target vertex
   * @param iProperties
   *          Map where keys are property names and values the expected values
   * @return
   */
  public Set<OIdentifiable> getOutEdgesHavingProperties(OIdentifiable iVertex, Map<String, Object> iProperties);

  /**
   * Retrieves the outgoing edges of vertex iVertex having the requested properties iProperties
   * 
   * @param iVertex
   *          Target vertex
   * @param iProperties
   *          Map where keys are property names and values the expected values
   * @return
   */
  public Set<OIdentifiable> getOutEdgesHavingProperties(OIdentifiable iVertex, Iterable<String> iProperties);

  public Set<OIdentifiable> getInEdges(OIdentifiable iVertex);

  public Set<OIdentifiable> getInEdges(OIdentifiable iVertex, String iLabel);

  /**
   * Retrieves the incoming edges of vertex iVertex having the requested properties iProperties
   * 
   * @param iVertex
   *          Target vertex
   * @param iProperties
   *          Map where keys are property names and values the expected values
   * @return
   */
  public Set<OIdentifiable> getInEdgesHavingProperties(OIdentifiable iVertex, Iterable<String> iProperties);

  /**
   * Retrieves the incoming edges of vertex iVertex having the requested properties iProperties set to the passed values
   * 
   * @param iVertex
   *          Target vertex
   * @param iProperties
   *          Map where keys are property names and values the expected values
   * @return
   */
  public Set<OIdentifiable> getInEdgesHavingProperties(ODocument iVertex, Map<String, Object> iProperties);

  public Set<OIdentifiable> filterEdgesByProperties(OMVRBTreeRIDSet iEdges, Iterable<String> iPropertyNames);

  public Set<OIdentifiable> filterEdgesByProperties(OMVRBTreeRIDSet iEdges, Map<String, Object> iProperties);

  public OClass createEdgeType(String iClassName);

  public OClass createEdgeType(String iClassName, String iSuperClassName);

  public OClass createEdgeType(String iClassName, OClass iSuperClass);

  public OClass getEdgeType(String iClassName);

  public OClass getEdgeBaseClass();

  public void checkEdgeClass(ODocument iEdge);

  public OClass checkEdgeClass(String iEdgeTypeName);

  public void checkEdgeClass(OClass iEdgeType);

  /**
   * Returns true if the document is an edge (its class is OGraphEdge or any subclasses)
   * 
   * @param iRecord
   *          Document to analyze.
   * @return true if the document is a edge (its class is OGraphEdge or any subclasses)
   */
  public boolean isEdge(ODocument iRecord);

  public void checkForGraphSchema();

  public LOCK_MODE getLockMode();

}