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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClusters;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.command.OCommandSelect;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OQuerySource {
  
  protected Iterable<? extends OIdentifiable> targetRecords;
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
    
    if(targetRecords != null){
      return targetRecords;
      
    }else if(targetClasse != null){
      final ODatabaseRecord db = getDatabase();
      db.checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, targetClasse);
      return new ORecordIteratorClass(db, (ODatabaseRecordAbstract)db, targetClasse, true);
      
    }else if(targetCluster != null){
      final ODatabaseRecord db = getDatabase();
      final int[] clIds = new int[]{db.getClusterIdByName(targetCluster)};
      return new ORecordIteratorClusters<ORecordInternal<?>>(db, db, clIds, false, false);
      
    }else{
      throw new OException("Source not supported yet");
    }
  }
  
  public Iterable<OIdentifiable> createIteratorFilterCandidates(Collection<OIdentifiable> ids) {
    if(targetRecords != null){
      //optimize list, clip results
      final Set<OIdentifiable> cross = new HashSet<OIdentifiable>(ids);
      cross.retainAll((Collection)targetRecords);
      return cross;
    }
    
    //can't not optimize, wrap the full iterator and exclude wrong results
    return new ClippedCollection(createIterator(), ids, true);
  }

  public Iterable<OIdentifiable> createIteratorFilterExcluded(Collection<OIdentifiable> ids) {
    if(targetRecords != null){
      //optimize list, clip results
      final Set<OIdentifiable> cross = new HashSet<OIdentifiable>((Collection)targetRecords);
      cross.removeAll(ids);
      return cross;
    }
    
    //can't not optimize, wrap the full iterator and exclude wrong results
    return new ClippedCollection(createIterator(), ids, false);
  }
  
  
  public void parse(OSQLParser.FromContext from) throws OCommandSQLParsingException {
    parse(from.source());
  }
  
  public void parse(OSQLParser.SourceContext candidate) throws OCommandSQLParsingException {
    
    if(candidate.orid() != null){
      //single identifier
      final OLiteral literal = visit(candidate.orid());
      final OIdentifiable id = (OIdentifiable) literal.evaluate(null, null);
      targetRecords = Collections.singleton(id);
      
    }else if(candidate.collection() != null){
      //collection of identifier
      final OCollection col = SQLGrammarUtils.visit(candidate.collection());
      targetRecords = (Iterable<? extends OIdentifiable>) col.evaluate(null, null);
      
    }else if(candidate.commandSelect() != null){
      //sub query
      final OCommandSelect sub = new OCommandSelect();
      sub.parse(candidate.commandSelect());
      targetRecords = sub;
      
    }else if(candidate.CLUSTER() != null){
      //cluster
      targetCluster = visitAsString(candidate.reference());
    }else if(candidate.INDEX()!= null){
      //index
      targetIndex = visitAsString(candidate.reference());
      
    }else if(candidate.DICTIONARY()!= null){
      //dictionnay
      final String key = visitAsString(candidate.reference());
      targetRecords = new ArrayList<OIdentifiable>();
      final OIdentifiable value = ODatabaseRecordThreadLocal.INSTANCE.get().getDictionary().get(key);
      if (value != null) {
        ((List<OIdentifiable>) targetRecords).add(value);
      }

    }else if(candidate.reference()!= null){
      //class
      targetClasse = visitAsString(candidate.reference());
    }else{
      throw new OCommandSQLParsingException("Unexpected source definition.");
    }
    
  }
    
  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
  
  /**
   * Unmodifiable collection result of a the merge of two collections.
   */
  private class ClippedCollection extends AbstractCollection<OIdentifiable>{

    private final Iterable<? extends OIdentifiable> source;
    private final boolean include;
    private final Collection<OIdentifiable> ids;
    
    private ClippedCollection(Iterable<? extends OIdentifiable> source, Collection<OIdentifiable> ids, boolean include){
      this.source = source;
      this.ids = ids;
      this.include = include;
    }
    
    @Override
    public Iterator<OIdentifiable> iterator() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int size() {
      int size = 0;
      final Iterator ite = iterator();
      while(ite.hasNext()){
        ite.next();
        size++;
      }
      return size;
    }
  
    private class ClippedIterator implements Iterator<OIdentifiable>{

      private final Iterator<? extends OIdentifiable> baseIte;
      private OIdentifiable next = null;

      public ClippedIterator() {
        baseIte = source.iterator();
      }
      
      @Override
      public boolean hasNext() {
        findNext();
        return next != null;
      }

      @Override
      public OIdentifiable next() {
        findNext();
        if(next == null){
          throw new NoSuchElementException("No more elements.");
        }
        OIdentifiable c = next;
        next = null;
        return c;
      }

      private void findNext(){
        if(next != null) return;
        
        while(next == null){
          if(baseIte.hasNext()){
            final OIdentifiable candidate = baseIte.next();
            if(include && ids.contains(candidate)){
              //included record
              next = candidate;
            }else if(!include && !ids.contains(candidate)){
              //not excluded result
              next = candidate;
            }
          }else{
            //no more records
            break;
          }
        }
      }
      
      @Override
      public void remove() {
        throw new UnsupportedOperationException("Not supported.");
      }
      
    }
    
  }
  
}
