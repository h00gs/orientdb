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
import java.io.IOException;
import java.util.Locale;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.storage.OCluster;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandAlterCluster extends OCommandAbstract implements OCommandDistributedReplicateRequest{

  public static final String KEYWORD_ALTER   = "ALTER";
  public static final String KEYWORD_CLUSTER = "CLUSTER";

  protected String  clusterName;
  protected int clusterId = -1;
  protected OCluster.ATTRIBUTES attribute;
  protected String value;
  
  public OCommandAlterCluster() {
  }

  public OCommandAlterCluster parse(final OCommandRequest iRequest) throws OCommandSQLParsingException {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandAlterClusterContext candidate = getCommand(iRequest, OSQLParser.CommandAlterClusterContext.class);
    
    if(candidate.number() != null){
      clusterId = visit(candidate.number()).intValue();
      attribute = OCluster.ATTRIBUTES.valueOf(visitAsString(candidate.reference(0)).toUpperCase(Locale.ENGLISH));
    }else{
      clusterName = visitAsString(candidate.reference(0));
      attribute = OCluster.ATTRIBUTES.valueOf(visitAsString(candidate.reference(1)).toUpperCase(Locale.ENGLISH));
    }    
    value = visit(candidate.cword(), iRequest);
    
    return this;
  }

  /**
   * Execute the ALTER CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (attribute == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final OCluster cluster = getCluster();

    if (cluster == null)
      throw new OCommandExecutionException("Cluster '" + clusterName + "' not found");

    if (clusterId > -1 && clusterName.equals(String.valueOf(clusterId))) {
      clusterName = cluster.getName();
    } else {
      clusterId = cluster.getId();
    }

    try {
      cluster.set(attribute, value);
    } catch (IOException ioe) {
      throw new OCommandExecutionException("Error altering cluster '" + clusterName + "'", ioe);
    }

    return null;
  }

  protected OCluster getCluster() {
    final ODatabaseRecord database = getDatabase();
    if (clusterId > -1) {
      return database.getStorage().getClusterById(clusterId);
    } else {
      return database.getStorage().getClusterById(database.getStorage().getClusterIdByName(clusterName));
    }
  }

  public String getSyntax() {
    return "ALTER CLUSTER <cluster-name>|<cluster-id> <attribute-name> <attribute-value>";
  }
  
}
