/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 * Copyright 2013 Geomatys.
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
package com.orientechnologies.orient.core.sql.command;

import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import com.orientechnologies.orient.core.storage.OCluster;

/**
 * SQL DROP CLASS command: Drops a class from the database. Cluster associated are removed too if are used exclusively by the
 * deleting class.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandDropClass extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  
  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_CLASS = "CLASS";
  private String className;
  
  public OCommandDropClass() {
  }

  public OCommandDropClass parse(final OCommandRequest iRequest) throws OCommandSQLParsingException {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandDropClassContext candidate = SQLGrammarUtils
            .getCommand(iRequest, OSQLParser.CommandDropClassContext.class);
    
    className = candidate.word().getText();
    final OClass schemaClass = database.getMetadata().getSchema().getClass(className);
    if (schemaClass == null){
      throw new OCommandSQLParsingException("Class '" + className + "' not found");
    }
    return this;
  }
  
  
  /**
   * Execute the DROP CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (className == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    final OClass oClass = database.getMetadata().getSchema().getClass(className);
    if (oClass == null)
      return null;

    for (final OIndex<?> oIndex : oClass.getClassIndexes()) {
      database.getMetadata().getIndexManager().dropIndex(oIndex.getName());
    }

    final OClass superClass = oClass.getSuperClass();
    final int[] clustersToIndex = oClass.getPolymorphicClusterIds();

    final String[] clusterNames = new String[clustersToIndex.length];
    for (int i = 0; i < clustersToIndex.length; i++) {
      clusterNames[i] = database.getClusterNameById(clustersToIndex[i]);
    }

    final int clusterId = oClass.getDefaultClusterId();

    ((OSchemaProxy) database.getMetadata().getSchema()).dropClassInternal(className);
    ((OSchemaProxy) database.getMetadata().getSchema()).saveInternal();
    database.getMetadata().getSchema().reload();

    deleteDefaultCluster(clusterId);

    if (superClass == null)
      return true;

    for (final OIndex<?> oIndex : superClass.getIndexes()) {
      for (final String clusterName : clusterNames)
        oIndex.getInternal().removeCluster(clusterName);

      OLogManager.instance().info("Index %s is used in super class of %s and should be rebuilt.", oIndex.getName(), className);
      oIndex.rebuild();
    }

    return true;
  }

  protected void deleteDefaultCluster(int clusterId) {
    final ODatabaseRecord database = getDatabase();
    OCluster cluster = database.getStorage().getClusterById(clusterId);
    if (cluster.getName().equalsIgnoreCase(className)) {
      if (isClusterDeletable(clusterId)) {
        database.getStorage().dropCluster(clusterId);
      }
    }
  }

  protected boolean isClusterDeletable(int clusterId) {
    final ODatabaseRecord database = getDatabase();
    for (OClass iClass : database.getMetadata().getSchema().getClasses()) {
      for (int i : iClass.getClusterIds()) {
        if (i == clusterId)
          return false;
      }
    }
    return true;
  }

  @Override
  public String getSyntax() {
    return "DROP CLASS <class>";
  }
  
}
