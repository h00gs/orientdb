/*
 * Copyright 2012 Orient Technologies.
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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.command.OCommandAlterClass;
import com.orientechnologies.orient.core.sql.command.OCommandAlterCluster;
import com.orientechnologies.orient.core.sql.command.OCommandAlterDatabase;
import com.orientechnologies.orient.core.sql.command.OCommandAlterProperty;
import com.orientechnologies.orient.core.sql.command.OCommandCreateClass;
import com.orientechnologies.orient.core.sql.command.OCommandCreateCluster;
import com.orientechnologies.orient.core.sql.command.OCommandCreateEdge;
import com.orientechnologies.orient.core.sql.command.OCommandCreateFunction;
import com.orientechnologies.orient.core.sql.command.OCommandCreateIndex;
import com.orientechnologies.orient.core.sql.command.OCommandCreateLink;
import com.orientechnologies.orient.core.sql.command.OCommandCreateProperty;
import com.orientechnologies.orient.core.sql.command.OCommandCreateVertex;
import com.orientechnologies.orient.core.sql.command.OCommandDropClass;
import com.orientechnologies.orient.core.sql.command.OCommandDropCluster;
import com.orientechnologies.orient.core.sql.command.OCommandDropIndex;
import com.orientechnologies.orient.core.sql.command.OCommandDropProperty;
import com.orientechnologies.orient.core.sql.command.OCommandFindReferences;
import com.orientechnologies.orient.core.sql.command.OCommandGrant;
import com.orientechnologies.orient.core.sql.command.OCommandInsert;
import com.orientechnologies.orient.core.sql.command.OCommandRebuildIndex;
import com.orientechnologies.orient.core.sql.command.OCommandRevoke;
import com.orientechnologies.orient.core.sql.command.OCommandSelect;
import com.orientechnologies.orient.core.sql.command.OCommandTruncateClass;
import com.orientechnologies.orient.core.sql.command.OCommandTruncateCluster;
import com.orientechnologies.orient.core.sql.command.OCommandTruncateRecord;

/**
 * Default command operator executor factory.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class ODefaultCommandExecutorSQLFactory implements OCommandExecutorSQLFactory {

  private static final Map<String, Class<? extends OCommandExecutor>> COMMANDS;

  static {

    final Map<String, Class<? extends OCommandExecutor>> commands = new HashMap<String, Class<? extends OCommandExecutor>>();
    
    // NEW ANTLR COMMANDS
    commands.put(OCommandAlterClass.KEYWORD_ALTER + " " + OCommandAlterClass.KEYWORD_CLASS,OCommandAlterClass.class);
    commands.put(OCommandAlterCluster.KEYWORD_ALTER + " " + OCommandAlterCluster.KEYWORD_CLUSTER,OCommandAlterCluster.class);
    commands.put(OCommandAlterDatabase.KEYWORD_ALTER + " " + OCommandAlterDatabase.KEYWORD_DATABASE,OCommandAlterDatabase.class);
    commands.put(OCommandAlterProperty.KEYWORD_ALTER + " " + OCommandAlterProperty.KEYWORD_PROPERTY,OCommandAlterProperty.class);
    commands.put(OCommandTruncateClass.KEYWORD_TRUNCATE + " " + OCommandTruncateClass.KEYWORD_CLASS,OCommandTruncateClass.class);
    commands.put(OCommandTruncateCluster.KEYWORD_TRUNCATE + " " + OCommandTruncateCluster.KEYWORD_CLUSTER,OCommandTruncateCluster.class);
    commands.put(OCommandTruncateRecord.KEYWORD_TRUNCATE + " " + OCommandTruncateRecord.KEYWORD_RECORD,OCommandTruncateRecord.class);
    commands.put(OCommandDropIndex.KEYWORD_DROP + " " + OCommandDropIndex.KEYWORD_INDEX,OCommandDropIndex.class);
    commands.put(OCommandDropCluster.KEYWORD_DROP + " " + OCommandDropCluster.KEYWORD_CLUSTER,OCommandDropCluster.class);
    commands.put(OCommandDropClass.KEYWORD_DROP + " " + OCommandDropClass.KEYWORD_CLASS, OCommandDropClass.class);
    commands.put(OCommandDropProperty.KEYWORD_DROP + " " + OCommandDropProperty.KEYWORD_PROPERTY,OCommandDropProperty.class);
    commands.put(OCommandGrant.KEYWORD_GRANT, OCommandGrant.class);
    commands.put(OCommandRevoke.KEYWORD_REVOKE, OCommandRevoke.class);
    commands.put(OCommandCreateClass.KEYWORD_CREATE + " " + OCommandCreateClass.KEYWORD_CLASS,OCommandCreateClass.class);
    commands.put(OCommandCreateCluster.KEYWORD_CREATE + " " + OCommandCreateCluster.KEYWORD_CLUSTER,OCommandCreateCluster.class);
    commands.put(OCommandCreateIndex.KEYWORD_CREATE + " " + OCommandCreateIndex.KEYWORD_INDEX,OCommandCreateIndex.class);
    commands.put(OCommandCreateProperty.KEYWORD_CREATE + " " + OCommandCreateProperty.KEYWORD_PROPERTY,OCommandCreateProperty.class);
    commands.put(OCommandCreateEdge.NAME, OCommandCreateEdge.class);
    commands.put(OCommandCreateVertex.NAME, OCommandCreateVertex.class);
    commands.put(OCommandCreateFunction.NAME, OCommandCreateFunction.class);
    commands.put(OCommandCreateLink.KEYWORD_CREATE + " " + OCommandCreateLink.KEYWORD_LINK,OCommandCreateLink.class);
    commands.put(OCommandRebuildIndex.KEYWORD_REBUILD + " " + OCommandRebuildIndex.KEYWORD_INDEX, OCommandRebuildIndex.class);
    commands.put(OCommandFindReferences.KEYWORD_FIND + " " + OCommandFindReferences.KEYWORD_REFERENCES, OCommandFindReferences.class);
    
    // NEW ANTLR COMMANDS : still uncomplete
    commands.put(OCommandInsert.KEYWORD_INSERT, OCommandInsert.class);
    commands.put(OCommandSelect.KEYWORD_SELECT, OCommandSelect.class);
    
    
    // OLD REPLACED COMMANDS 
//    commands.put(OCommandExecutorSQLInsert.KEYWORD_INSERT, OCommandExecutorSQLInsert.class);
    //commands.put(OCommandExecutorSQLSelect.KEYWORD_SELECT, OCommandExecutorSQLSelect.class);
    
    // OLD MANUAL PARSING COMMANDS
    commands.put(OCommandExecutorSQLTraverse.KEYWORD_TRAVERSE, OCommandExecutorSQLTraverse.class);
    commands.put(OCommandExecutorSQLUpdate.KEYWORD_UPDATE, OCommandExecutorSQLUpdate.class);
    commands.put(OCommandExecutorSQLDelete.NAME, OCommandExecutorSQLDelete.class);
    commands.put(OCommandExecutorSQLDeleteEdge.NAME, OCommandExecutorSQLDeleteEdge.class);
    commands.put(OCommandExecutorSQLDeleteVertex.NAME, OCommandExecutorSQLDeleteVertex.class);
    commands.put(OCommandExecutorSQLExplain.KEYWORD_EXPLAIN, OCommandExecutorSQLExplain.class);

    COMMANDS = Collections.unmodifiableMap(commands);
  }

  /**
   * {@inheritDoc}
   */
  public Set<String> getCommandNames() {
    return COMMANDS.keySet();
  }

  /**
   * {@inheritDoc}
   */
  public OCommandExecutor createCommand(final String name) throws OCommandExecutionException {
    final Class<? extends OCommandExecutor> clazz = COMMANDS.get(name);

    if (clazz == null) {
      throw new OCommandExecutionException("Unknowned command name :" + name);
    }

    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw new OCommandExecutionException("Error in creation of command " + name
          + "(). Probably there is not an empty constructor or the constructor generates errors", e);
    }
  }

}
