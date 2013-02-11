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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClusters;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.command.OCommandSelect;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  public String getTargetCluster() {
    return targetCluster;
  }

  public String getTargetIndex() {
    return targetIndex;
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
  
  public void parse(OSQLParser.FromContext candidate) throws OCommandSQLParsingException {

    if(candidate.identifier() != null){
      //single identifier
      final OLiteral literal = SQLGrammarUtils.visit(candidate.identifier());
      final OIdentifiable id = (OIdentifiable) literal.evaluate(null, null);
      targetRecords = Collections.singleton(id);
      
    }else if(candidate.collection() != null){
      //collection of identifier
      final OCollection col = SQLGrammarUtils.visit(candidate.collection());
      targetRecords = col.evaluate(null, null);
      
    }else if(candidate.commandSelect() != null){
      //sub query
      final OCommandSelect sub = new OCommandSelect();
      sub.parse(candidate.commandSelect());
      targetRecords = sub;
      
    }else if(candidate.CLUSTER() != null){
      //cluster
      targetCluster = candidate.word().getText();
    }else if(candidate.INDEX()!= null){
      //index
      targetIndex = candidate.word().getText();
      
    }else if(candidate.DICTIONARY()!= null){
      //dictionnay
      final String key = candidate.word().getText();
      targetRecords = new ArrayList<OIdentifiable>();
      final OIdentifiable value = ODatabaseRecordThreadLocal.INSTANCE.get().getDictionary().get(key);
      if (value != null) {
        ((List<OIdentifiable>) targetRecords).add(value);
      }

    }else if(candidate.word()!= null){
      //class
      targetClasse = candidate.word().getText();
    }else{
      throw new OCommandSQLParsingException("Unexpected source definition.");
    }
    
  }
    
  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
  
}
