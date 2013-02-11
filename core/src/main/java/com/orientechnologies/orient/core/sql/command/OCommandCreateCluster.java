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

import com.orientechnologies.orient.core.Orient;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;

/**
 * SQL CREATE CLUSTER command: Creates a new cluster.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandCreateCluster extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  
  public static final String KEYWORD_CREATE = "CREATE";
  public static final String KEYWORD_CLUSTER = "CLUSTER";
  public static final String KEYWORD_DATASEGMENT = "DATASEGMENT";
  public static final String KEYWORD_LOCATION = "LOCATION";
  public static final String KEYWORD_POSITION = "POSITION";

  private String clusterName;
  private String clusterType;
  private String dataSegmentName = "default";
  private String location = "default";
  private String position = "append";
  
  public OCommandCreateCluster() {
  }

  public OCommandCreateCluster parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandCreateClusterContext candidate = SQLGrammarUtils
            .getCommand(iRequest, OSQLParser.CommandCreateClusterContext.class);
    
    int i=0;
    clusterName = candidate.word(i++).getText();
    clusterType = candidate.word(i++).getText();
    
    if(candidate.DATASEGMENT() != null){
      dataSegmentName = candidate.word(i++).getText();
    }
    if(candidate.LOCATION() != null){
      location = candidate.word(i++).getText();
    }
    if(candidate.POSITION()!= null){
      position = candidate.word(i++).getText();
    }
    
    final int clusterId = database.getStorage().getClusterIdByName(clusterName);
    if (clusterId > -1){
      throw new OCommandSQLParsingException("Cluster '" + clusterName + "' already exists");
    }
    final int dataId = database.getStorage().getDataSegmentIdByName(dataSegmentName);
    if (dataId == -1){
      throw new OCommandSQLParsingException("Data segment '" + dataSegmentName + "' does not exists");
    }
    if (!Orient.instance().getClusterFactory().isSupported(clusterType)){
      throw new OCommandSQLParsingException("Cluster type '" + clusterType + "' is not supported");
    }

    return this;
  }

  /**
   * Execute the CREATE CLUSTER.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clusterName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();

    return database.addCluster(clusterType, clusterName, location, dataSegmentName);
  }

  @Override
  public String getSyntax() {
    return "CREATE CLUSTER <name> <type> [DATASEGMENT <data-segment>|default] [LOCATION <path>|default] [POSITION <position>|append]";
  }
  
}
