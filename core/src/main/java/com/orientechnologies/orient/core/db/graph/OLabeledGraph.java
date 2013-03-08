package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.orient.core.db.graph.OPropertyGraph.LOCK_MODE;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

public interface OLabeledGraph {

  public enum DIRECTION {
    BOTH, IN, OUT
  }
  
  public long countVertexes();

  public Iterable<ODocument> browseVertices();

  public Iterable<ODocument> browseVertices(boolean iPolymorphic);

  public Iterable<ODocument> browseElements(String iClass, boolean iPolymorphic);

  public ODocument createVertex();

  public ODocument createVertex(String iClassName);

  public ODocument createVertex(String iClassName, Object... iFields);

  public ODocument createEdge(ORID iSourceVertexRid, ORID iDestVertexRid);

  public ODocument createEdge(ORID iSourceVertexRid, ORID iDestVertexRid, String iClassName);

  public ODocument createEdge(ODocument iSourceVertex, ODocument iDestVertex);

  public ODocument createEdge(ODocument iOutVertex, ODocument iInVertex, String iClassName);

  public boolean removeEdge(OIdentifiable iEdge);

  public boolean removeVertex(OIdentifiable iVertex);

  public ODocument getInVertex(OIdentifiable iEdge);

  public ODocument getOutVertex(OIdentifiable iEdge);

  public ODocument getRoot(String iName);

  public ODocument getRoot(String iName, String iFetchPlan);

  public OLabeledGraph setRoot(String iName, ODocument iNode);

  public OClass createVertexType(String iClassName);

  public OClass createVertexType(String iClassName, String iSuperClassName);

  public OClass createVertexType(String iClassName, OClass iSuperClass);

  public OClass getVertexType(String iClassName);

  public boolean isSafeMode();

  public void setSafeMode(boolean safeMode);

  public OClass getVertexBaseClass();

  public void checkVertexClass(ODocument iVertex);

  public OClass checkVertexClass(String iVertexTypeName);

  public void checkVertexClass(OClass iVertexType);

  public boolean isUseCustomTypes();

  public void setUseCustomTypes(boolean useCustomTypes);

  /**
   * Returns true if the document is a vertex (its class is OGraphVertex or any subclasses)
   * 
   * @param iRecord
   *          Document to analyze.
   * @return true if the document is a vertex (its class is OGraphVertex or any subclasses)
   */
  public boolean isVertex(ODocument iRecord);

  public String getType();

  public void setLockMode(LOCK_MODE lockMode);

}
