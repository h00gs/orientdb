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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.io.OUtils;
import com.orientechnologies.orient.core.db.graph.edge.OBidirectionalEdgeDocument;
import com.orientechnologies.orient.core.db.graph.vertex.OVertexDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Migration tool. Keeps updated the graph with latest OrientDB release.
 * 
 * @author Luca Garulli
 * 
 */
public class OGraphDatabaseCompacter {
  private long convertedVertices;
  private long updatedEdges;
  private long downgradedEdges;
  private long collection2links;
  private long propertyRemoved;
  private long edgeClassesCreated;

  public static void main(final String[] iArgs) {
    if (iArgs.length < 1) {
      System.err.println("Error: wrong parameters. Syntax: <database-url> [<user> <password>] [<options>]");
      System.err.println("Where:");
      System.err.println("- <database-url> is the database url, example: local:/temp/db");
      System.err.println("- <user> is the user name");
      System.err.println("- <password> is the user's password");
      System.err.println("- <options> can be:");
      System.err.println("  -labelClass to create automatically one class per Label found");
      System.err.println("  -labelName <name> to use different field name for labels. Default is 'label'");
      return;
    }

    final String dbURL = iArgs[0];
    final String user = iArgs.length > 1 ? iArgs[1] : null;
    final String password = iArgs.length > 2 ? iArgs[2] : null;

    final Map<String, Object> options = new HashMap<String, Object>();

    for (int i = 3; i < iArgs.length; ++i) {
      final String arg = iArgs[i];
      if (arg.equalsIgnoreCase("-labelClass"))
        options.put("labelClass", true);
      else if (arg.equalsIgnoreCase("-labelName"))
        options.put("labelName", iArgs[++i]);
    }

    new OGraphDatabaseCompacter().compact(dbURL, user, password, options);
  }

  public void compact(final String dbURL, final String user, final String password) {
    compact((OGraphDatabase) new OGraphDatabase(dbURL).open(user, password), null);
  }

  public void compact(final String dbURL, final String user, final String password, final Map<String, Object> options) {
    compact((OGraphDatabase) new OGraphDatabase(dbURL).open(user, password), options);
  }

  public void compact(final OAbstractPropertyGraph db, final Map<String, Object> iOptions) {
    System.out.println("Migration of database started...");
    final long start = System.currentTimeMillis();

    final Map<String, Object> options = new HashMap<String, Object>();
    options.put("labelClass", false);
    options.put("labelName", OPropertyGraph.LABEL);

    if (iOptions != null)
      for (Entry<String, Object> option : iOptions.entrySet())
        options.put(option.getKey(), option.getValue());

    try {
      // CONVERT THE SCHEMA
      convertSchema(db);

      convertedVertices = 0;
      for (ODocument doc : db.browseVertices()) {
        if (convertVertexEdges(db, doc, options)) {
          doc.save();
          convertedVertices++;
        }
      }

      System.out.println(String.format("Migration complete in %d seconds.", (System.currentTimeMillis() - start) / 1000));

      System.out.println("Full report:\n");

      System.out.println(String.format("- Vertices updated.........: %d", convertedVertices));
      System.out.println(String.format("- Edges updated............: %d", updatedEdges));
      System.out.println(String.format("- Edges downgraded to links: %d", downgradedEdges));
      System.out.println(String.format("- Edge classes created.....: %d", edgeClassesCreated));
      System.out.println(String.format("- Collection to links......: %d", collection2links));
      System.out.println(String.format("- Properties removed.......: %d", propertyRemoved));

    } finally {
      db.close();
    }
  }

  protected boolean convertVertexEdges(final OAbstractPropertyGraph db, final ODocument doc, final Map<String, Object> options) {
    boolean changed = false;

    if (convertEdges(db, doc, OVertexDocument.VERTEX_FIELD_OUT, options))
      changed = true;
    if (convertEdges(db, doc, OVertexDocument.VERTEX_FIELD_IN, options))
      changed = true;

    return changed;
  }

  @SuppressWarnings("unchecked")
  protected boolean convertEdges(final OAbstractPropertyGraph db, final ODocument iVertex, final String iPropertyBaseName,
      final Map<String, Object> options) {

    final boolean labelClass = (Boolean) options.get("labelClass");
    final String labelName = (String) options.get("labelName");

    // CONTAINER TO USE IN CASE OUT/IN IS USED TO AVOID INFINITE LOOPS
    final OMVRBTreeRIDSet genericFieldContainer = new OMVRBTreeRIDSet(iVertex);

    final Object origEdges = iVertex.field(iPropertyBaseName);
    if (origEdges != null) {
      if (origEdges instanceof Collection<?>)
        convertEdgeCollection(db, iVertex, iPropertyBaseName, labelClass, labelName, (Collection<OIdentifiable>) origEdges,
            genericFieldContainer);
      else if (origEdges instanceof OIdentifiable) {
        final Object edge = convertEdge(db, iVertex, iPropertyBaseName, labelName, (OIdentifiable) origEdges);
        assignEdge(iVertex, iPropertyBaseName, edge, genericFieldContainer);
      }
    }

    if (genericFieldContainer.isEmpty()) {
      // REMOVE THE FIELD
      iVertex.removeField(iPropertyBaseName);
      propertyRemoved++;
    } else if (genericFieldContainer.size() == 1)
      // SET THE LINK ONLY
      iVertex.field(iPropertyBaseName, genericFieldContainer.iterator().next());
    else
      iVertex.field(iPropertyBaseName, genericFieldContainer);

    return true;
  }

  protected boolean convertEdgeCollection(final OAbstractPropertyGraph db, final ODocument iVertex, final String iPropertyBaseName,
      final boolean labelClass, final String labelName, final Collection<OIdentifiable> origContainer,
      final OMVRBTreeRIDSet iGenericEdgeContainer) {

    if (origContainer == null || origContainer.size() == 0) {
      // EMPTY: REMOVE IT
      iVertex.removeField(iPropertyBaseName);
      propertyRemoved++;
      return true;
    }

    // CHECK NEED CONVERSION
    boolean convert = false;
    for (Object o : origContainer)
      if (isEdgeNeedConversion(db, labelClass, labelName, o))
        convert = true;

    if (!convert)
      // NO CONVERSION
      return false;

    // START CONVERSION OF EDGES
    for (OIdentifiable o : origContainer) {
      Object edge = convertEdge(db, iVertex, iPropertyBaseName, labelName, o);
      assignEdge(iVertex, iPropertyBaseName, edge, iGenericEdgeContainer);
    }

    return true;
  }

  protected boolean assignEdge(final ODocument iVertex, final String iPropertyBaseName, Object edge,
      final OMVRBTreeRIDSet iGenericContainer) {
    if (edge == null)
      return false;

    final Object[] edgeValues = (Object[]) edge;
    final String propertyName = edgeValues[1].toString();

    boolean assignField;
    Object edgeContainer;
    if (propertyName.equals(iPropertyBaseName)) {
      // SAME OF THE ORIGINAL ONE: USE A TEMP CONTAINER TO AVOID INFINITE LOOP
      edgeContainer = iGenericContainer;
      assignField = false;
    } else {
      edgeContainer = iVertex.field(propertyName);
      assignField = true;
    }

    if (edgeContainer == null) {
      // CREATE SINGLE VALUE
      edgeContainer = edgeValues[0];
    } else if (edgeContainer instanceof OIdentifiable) {
      if (!edgeContainer.equals(edgeValues[0])) {
        // CONVERT IN A COLLECTION OF EDGES
        final OMVRBTreeRIDSet set = new OMVRBTreeRIDSet(iVertex);
        set.add((OIdentifiable) edgeContainer);
        set.add((OIdentifiable) edgeValues[0]);
        edgeContainer = set;
      } else
        assignField = false;

    } else if (edgeContainer instanceof OMVRBTreeRIDSet) {
      // JUST ADD THE EDGE TO THE EXISTENT ONE
      ((OMVRBTreeRIDSet) edgeContainer).add((OIdentifiable) edgeValues[0]);
      assignField = false;
    } else
      throw new ODatabaseException("Cannot migrate the graph database because the property '" + iPropertyBaseName
          + "' contains the invalid object: " + edgeContainer);

    if (assignField)
      iVertex.field(propertyName, edgeContainer);

    return true;
  }

  protected Object convertEdge(final OAbstractPropertyGraph db, final ODocument iVertex, final String iPropertyBaseName,
      final String labelName, OIdentifiable o) {
    if (o == null)
      return null;

    String propertyName = iPropertyBaseName;
    String className = db.getEdgeBaseClass().getName();

    OIdentifiable newEdge = o;

    final ODocument doc = o.getRecord();
    if (doc == null)
      return null;

    if (db.isEdge(doc)) {
      final ODocument edge = (ODocument) o;
      final String label = edge.field(labelName);

      if (!db.getEdgeBaseClass().getName().equals(edge.getSchemaClass().getName())) {
        // CUSTOM EDGE CLASS
        if (label != null && !label.equalsIgnoreCase(edge.getSchemaClass().getName()))
          throw new ODatabaseException("Cannot migrate the graph database because an edge is of class '"
              + edge.getSchemaClass().getName() + "' and contains a different label: " + label);

        // USE THE CLASS NAME
        propertyName += edge.getSchemaClass().getName();
      } else if (label != null) {
        // USE THE LABEL
        className = OUtils.camelCase(label);
        propertyName += className;
      }

      // CAN CONVERT IT TO JUST A DIRECT LINK TO THE VERTEX?
      boolean downgradeToLink = true;
      for (String n : edge.fieldNames()) {
        if (!n.equals(OVertexDocument.VERTEX_FIELD_IN) && !n.equals(OVertexDocument.VERTEX_FIELD_OUT)
            && !n.equals(OPropertyGraph.LABEL)) {
          // ADDITION AL PROPERTY: CANNOT DOWNGRADE IT
          downgradeToLink = false;
          break;
        }
      }

      if (downgradeToLink) {
        // GET AS NEW EDGE THE LINK TO THE TARGET VERTEX
        if (iVertex.equals(edge.field(OBidirectionalEdgeDocument.EDGE_FIELD_IN)))
          newEdge = edge.field(OBidirectionalEdgeDocument.EDGE_FIELD_OUT);
        else if (iVertex.equals(edge.field(OBidirectionalEdgeDocument.EDGE_FIELD_OUT)))
          newEdge = edge.field(OBidirectionalEdgeDocument.EDGE_FIELD_IN);
        else
          throw new ODatabaseException("Cannot migrate the graph database because the edge " + edge
              + " contains an invalid reference to the linked vertex " + iVertex);

        downgradedEdges++;

        edge.delete();
      } else if (!edge.getClassName().equalsIgnoreCase(className)) {
        // CHECK THE CLASS OR CREATE IT
        OClass edgeClass = db.getEdgeType(className);
        if (edgeClass == null) {
          // CREATE THE EDGE CLASS FIRST TIME ONLY
          edgeClass = db.createEdgeType(className);
          edgeClassesCreated++;
        }

        // CREATE A COPY OF THE EDGE OF THE NEW CLASS COPYING ALL THE PROPERTIES BUT LABEL BECAUSE THE CLASS NAME ASSUMES THE SAME
        // MEANING
        newEdge = new ODocument(className);
        for (String n : edge.fieldNames())
          if (!n.equals(labelName))
            ((ODocument) newEdge).field(n, edge.rawField(n));
        ((ODocument) newEdge).save();
      }
    }

    return new Object[] { newEdge, propertyName };
  }

  protected boolean isEdgeNeedConversion(final OAbstractPropertyGraph db, final boolean labelClass, final String labelName, Object o) {
    if (o instanceof ODocument) {
      final ODocument edge = (ODocument) o;
      if (labelClass && (edge.containsField(labelName) || !db.getEdgeBaseClass().equals(edge.getSchemaClass())))
        return true;

      if (edge.fieldNames().length == 2)
        // NO PROPERTY, CANDIDATE TO DOWNGRADE IT TO LINK
        return true;
    }
    return false;
  }

  protected void convertSchema(final OAbstractPropertyGraph db) {
    if (db.getVertexBaseClass().existsProperty(OVertexDocument.VERTEX_FIELD_OUT))
      db.getVertexBaseClass().dropProperty(OVertexDocument.VERTEX_FIELD_OUT);

    if (db.getVertexBaseClass().existsProperty(OVertexDocument.VERTEX_FIELD_IN))
      db.getVertexBaseClass().dropProperty(OVertexDocument.VERTEX_FIELD_IN);

    if (db.getEdgeBaseClass().existsProperty(OBidirectionalEdgeDocument.EDGE_FIELD_IN))
      db.getEdgeBaseClass().dropProperty(OBidirectionalEdgeDocument.EDGE_FIELD_IN);
    if (db.getEdgeBaseClass().existsProperty(OBidirectionalEdgeDocument.EDGE_FIELD_OUT))
      db.getEdgeBaseClass().dropProperty(OBidirectionalEdgeDocument.EDGE_FIELD_OUT);
  }
}
