/*
 * Copyright 2013 Orient Technologies.
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.model;

import java.util.*;

import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClusters;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.command.OCommandSelect;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OQuerySource {
  
  protected Iterable<OIdentifiable> targetRecords;
  protected String targetCluster;
  protected String targetClasse;
  protected String targetIndex;
  
  public OQuerySource() {
    super();
  }

  public String getTargetClasse() {
    return targetClasse;
  }

  public void setTargetClasse(String targetClasse) {
    this.targetClasse = targetClasse;
  }
  
  public String getTargetCluster() {
    return targetCluster;
  }

  public void setTargetCluster(String targetCluster) {
    this.targetCluster = targetCluster;
  }
  
  public String getTargetIndex() {
    return targetIndex;
  }

  public void setTargetIndex(String targetIndex) {
    this.targetIndex = targetIndex;
  }
  
  public Iterable<? extends OIdentifiable> getTargetRecords() {
    return targetRecords;
  }
  
  public Iterable<? extends OIdentifiable> createIterator(){
    return createIterator(null, null);
  }
  
  public Iterable<? extends OIdentifiable> createIterator(ORID start, ORID end){
    
    if(targetRecords != null){
      return targetRecords;
      
    }else if(targetClasse != null){
      final ODatabaseRecord db = getDatabase();
      db.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, targetClasse);
      ORecordIteratorClass ite = new ORecordIteratorClass(db, (ODatabaseRecordAbstract)db, targetClasse, true);
      if(start != null || end != null){
          ite = (ORecordIteratorClass) ite.setRange(start, end);
      }
      
      return ite;
    }else if(targetCluster != null){
      final ODatabaseRecord db = getDatabase();
      final int[] clIds = new int[]{db.getClusterIdByName(targetCluster)};
      ORecordIteratorClusters ite =  new ORecordIteratorClusters<ORecordInternal<?>>(db, db, clIds, false, false).setRange(start, end);
      if(start != null || end != null){
          ite = (ORecordIteratorClass) ite.setRange(start, end);
      }
      return ite;
    }else if(targetIndex != null){
        return searchInIndex();
    }else{
      throw new OException("Source not supported yet");
    }
  }

  private Iterable<? extends OIdentifiable> searchInIndex() {
    final OIndex<Object> index = (OIndex<Object>) getDatabase().getMetadata().getIndexManager().getIndex(targetIndex);

    if (index == null){
      throw new OCommandExecutionException("Target index '" + targetIndex + "' not found");
    }

    final List<OIdentifiable> indexEntries = new ArrayList<OIdentifiable>();

    // nothing was added yet, so index definition for manual index was not calculated
    if (index.getDefinition() == null){
      return indexEntries;
    }

    final OIndexInternal<?> indexInternal = index.getInternal();
    if (indexInternal instanceof OSharedResource){
      ((OSharedResource) indexInternal).acquireExclusiveLock();
    }

    try {
      // ADD ALL THE ITEMS AS RESULT
      for (Iterator<Map.Entry<Object, Object>> it = index.iterator(); it.hasNext();) {
        final Map.Entry<Object, Object> current = it.next();

        if (current.getValue() instanceof Collection<?>) {
          for (OIdentifiable identifiable : ((OMVRBTreeRIDSet) current.getValue())){
            final ODocument doc = createIndexEntryAsDocument(current.getKey(), identifiable.getIdentity());
            indexEntries.add(doc);
          }
        }else{
          final ODocument doc = createIndexEntryAsDocument(current.getKey(), (OIdentifiable) current.getValue());
          indexEntries.add(doc);
        }
      }
    } finally {
      if (indexInternal instanceof OSharedResource){
        ((OSharedResource) indexInternal).releaseExclusiveLock();
      }
    }

    return indexEntries;
  }

  private static ODocument createIndexEntryAsDocument(final Object iKey, final OIdentifiable iValue) {
    final ODocument doc = new ODocument().setOrdered(true);
    doc.field("key", iKey);
    doc.field("rid", iValue);
    doc.unsetDirty();
    return doc;
  }

  public void parse(OSQLParser.FromContext from) throws OCommandSQLParsingException {
    parse(from.source());
  }
  
  public void parse(OSQLParser.SourceContext candidate) throws OCommandSQLParsingException {
    
    if(candidate.orid() != null){
      //single identifier
      final OLiteral literal = visit(candidate.orid());
      final OIdentifiable id = (OIdentifiable) literal.evaluate(null, null);
      targetRecords = new TreeSet<OIdentifiable>();
      ((TreeSet<OIdentifiable>) targetRecords).add(id);
      
    }else if(candidate.collection() != null){
      //collection of identifier
      final OCollection col = SQLGrammarUtils.visit(candidate.collection());
      final Collection c = (Collection) col.evaluate(null, null);
      targetRecords = new TreeSet<OIdentifiable>();
      ((TreeSet)targetRecords).addAll(c);
      
    }else if(candidate.commandSelect() != null){
      //sub query
      final OCommandSelect sub = new OCommandSelect();
      sub.parse(candidate.commandSelect());
      targetRecords = sub;
      
    }else if(candidate.CLUSTER() != null){
      //cluster
      final OExpression exp = visit(candidate.expression());
      targetCluster = toString(exp);
    }else if(candidate.INDEX()!= null){
      //index
      final OExpression exp = visit(candidate.expression());
      targetIndex = toString(exp);
      
    }else if(candidate.DICTIONARY()!= null){
      //dictionnay
      final OExpression exp = visit(candidate.expression());
      final String key = toString(exp);
      targetRecords = new TreeSet<OIdentifiable>();
      final OIdentifiable value = ODatabaseRecordThreadLocal.INSTANCE.get().getDictionary().get(key);
      if (value != null) {
        ((List<OIdentifiable>) targetRecords).add(value);
      }

    }else if(candidate.expression()!= null){
      //class
      final OExpression exp = visit(candidate.expression());
      targetClasse = toString(exp);
    }else{
      throw new OCommandSQLParsingException("Unexpected source definition.");
    }
    
  }

  private static String toString(OExpression exp){
    if(exp instanceof OName){
      return ((OName)exp).getName();
    }else if(exp instanceof OPath){
      //path ?
      OPath p = (OPath) exp;
      final StringBuilder sb = new StringBuilder();
      sb.append(toString(p.getLeft()));
      sb.append('.');
      sb.append(toString(p.getRight()));
      return sb.toString();
    }else{
      throw new OCommandSQLParsingException("Unexpected name :"+exp);
    }
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
    
}
