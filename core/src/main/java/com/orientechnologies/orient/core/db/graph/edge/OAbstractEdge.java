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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

/**
 * Bidirectional link managed as a pair of raw LINK objects.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OAbstractEdge implements OBidirectionalEdge {
  @SuppressWarnings("unchecked")
  protected void dropEdgeFromVertex(final ODocument iEdge, final ODocument iVertex, final String iFieldName,
      final Object iFieldValue) {
    if (iFieldValue == null) {
      // NO EDGE? WARN
      OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s while removing the edge %s",
          iVertex.getIdentity(), iFieldName, iEdge.getIdentity());

    } else if (iFieldValue instanceof OIdentifiable) {
      // FOUND A SINGLE ITEM: JUST REMOVE IT

      if (iFieldValue.equals(iEdge))
        iVertex.field(iFieldName, (OIdentifiable) null);
      else
        // NO EDGE? WARN
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s link while removing the edge %s",
            iVertex.getIdentity(), iFieldName, iEdge.getIdentity());

    } else if (iFieldValue instanceof OMVRBTreeRIDSet) {
      // ALREADY A SET: JUST REMOVE THE NEW EDGE
      if (!((OMVRBTreeRIDSet) iFieldValue).remove(iEdge))
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s set while removing the edge %s",
            iVertex.getIdentity(), iFieldName, iEdge.getIdentity());
    } else if (iFieldValue instanceof Collection<?>) {
      // CONVERT COLLECTION IN TREE-SET AND REMOVE THE EDGE
      final OMVRBTreeRIDSet out = new OMVRBTreeRIDSet(iVertex, (Collection<OIdentifiable>) iFieldValue);
      if (!out.remove(iEdge))
        OLogManager.instance().warn(this, "Edge not found in vertex's property %s.%s collection while removing the edge %s",
            iVertex.getIdentity(), iFieldName, iEdge.getIdentity());
      else
        iVertex.field(iFieldName, out);
    } else
      throw new IllegalStateException("Wrong type found in the field '" + iFieldName + "': " + iFieldValue.getClass());
  }
}
