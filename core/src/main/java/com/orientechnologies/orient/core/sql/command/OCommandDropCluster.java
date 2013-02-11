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

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;

/**
 * SQL DROP CLUSTER command: Drop a cluster from the database
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandDropCluster extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  
  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_CLUSTER = "CLUSTER";
  private String clusterName;
  
  public OCommandDropCluster() {
  }

  public OCommandDropCluster parse(final OCommandRequest iRequest) throws OCommandSQLParsingException {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandDropClusterContext candidate = SQLGrammarUtils
            .getCommand(iRequest, OSQLParser.CommandDropClusterContext.class);
    
    clusterName = candidate.word().getText();
    return this;
  }
  
  /**
   * Execute the DROP CLUSTER.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clusterName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();

    // CHECK IF ANY CLASS IS USING IT
    final int clusterId = database.getStorage().getClusterIdByName(clusterName);
    for (OClass iClass : database.getMetadata().getSchema().getClasses()) {
      for (int i : iClass.getClusterIds()) {
        if (i == clusterId)
          // IN USE
          return false;
      }
    }

    database.dropCluster(clusterId);
    return true;
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
    return "DROP CLUSTER <cluster>";
  }
  
}
