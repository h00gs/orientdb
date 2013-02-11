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
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import java.util.List;

/**
 * SQL CREATE CLASS command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandCreateClass extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  public static final String KEYWORD_CREATE   = "CREATE";
  public static final String KEYWORD_CLASS    = "CLASS";
  public static final String KEYWORD_EXTENDS  = "EXTENDS";
  public static final String KEYWORD_ABSTRACT = "ABSTRACT";
  public static final String KEYWORD_CLUSTER  = "CLUSTER";

  private String className;
  private OClass superClass;
  private int[] clusterIds;

  public OCommandCreateClass() {
  }

  public OCommandCreateClass parse(final OCommandRequest iRequest) throws OCommandSQLParsingException {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandCreateClassContext candidate = SQLGrammarUtils
            .getCommand(iRequest, OSQLParser.CommandCreateClassContext.class);
    
    int i=0;
    className = candidate.word(i++).getText();
    if (database.getMetadata().getSchema().existsClass(className)){
        throw new OCommandSQLParsingException("Class " + className + " already exist");
    }
    
    if(candidate.EXTENDS() != null){
      final String superClassName = candidate.word(i++).getText();
      if (!database.getMetadata().getSchema().existsClass(superClassName)){
          throw new OCommandSQLParsingException("Super-class " + superClassName + " do not exist");
      }
      superClass = database.getMetadata().getSchema().getClass(superClassName);
    }
    if(candidate.CLUSTER() != null){
      final List<OSQLParser.NumberOrWordContext> lst = candidate.numberOrWord();
      clusterIds = new int[lst.size()];
      for(int k=0,n=lst.size();k<n;k++){
        final String clusterName = lst.get(k).getText();
        if (Character.isDigit(clusterName.charAt(0))) {
          // GET CLUSTER ID FROM NAME
          clusterIds[k] = Integer.parseInt(clusterName);
        } else {
          // GET CLUSTER ID
          clusterIds[k] = database.getStorage().getClusterIdByName(clusterName);
          System.out.println(clusterName+" "+clusterIds[k]);
        }
        if(clusterIds[k] == -1){
          throw new OCommandSQLParsingException("Cluster "+clusterName+"with id " + clusterIds[k] + " does not exists");
        }
        try {
          database.getStorage().getClusterById(clusterIds[k]);
        } catch (Exception e) {
          throw new OCommandSQLParsingException("Cluster with id " + clusterIds[k] + " does not exists");
        }
      }
    }
    
    if(candidate.ABSTRACT() != null){
      clusterIds = new int[] { -1 };
    }
    
    return this;
  }

  
  /**
   * Execute the CREATE CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (className == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    if (database.getMetadata().getSchema().existsClass(className))
      throw new OCommandExecutionException("Class " + className + " already exists");

    final OClassImpl sourceClass = (OClassImpl) ((OSchemaProxy) database.getMetadata().getSchema()).createClassInternal(className,
        superClass, clusterIds);
    sourceClass.saveInternal();

    return database.getMetadata().getSchema().getClasses().size();
  }

  @Override
  public String getSyntax() {
    return "CREATE CLASS <class> [EXTENDS <super-class>] [CLUSTER <clusterId>*] [ABSTRACT]";
  }
  
}
