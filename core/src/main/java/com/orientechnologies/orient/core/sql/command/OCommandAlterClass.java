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

import com.orientechnologies.common.exception.OException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.ATTRIBUTES;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.parser.OSQL;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandAlterClass extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest{
  public static final String KEYWORD_ALTER = "ALTER";
  public static final String KEYWORD_CLASS = "CLASS";

  private String             className;
  private ATTRIBUTES         attribute;
  private String             value;

  public OCommandAlterClass() {
  }

  public OCommandAlterClass(String className, ATTRIBUTES attribute, String value) {
    this.className = className;
    this.attribute = attribute;
    this.value = value;
  }

  public OCommandAlterClass parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    //ANTLR parsing
    final String sql = ((OCommandRequestText) iRequest).getText();
    System.err.println("|||||||||||||||||||||| "+sql);
    final ParseTree tree = OSQL.compileExpression(sql);
    if(!(tree instanceof OSQLParser.CommandContext)){
      throw new OException("Parse error, query is not a valid INSERT INTO.");
    }
    
    final Object commandTree = ((OSQLParser.CommandContext)tree).getChild(0);
    final OSQLParser.CommandAlterClassContext candidate;
    if(commandTree instanceof OSQLParser.CommandAlterClassContext){
      candidate = (OSQLParser.CommandAlterClassContext)commandTree;
    }else{
      throw new OException("Unknowned command " + tree.getClass()+" "+tree);
    }
    
    className = candidate.word(0).getText();
    final String attributeAsString = candidate.word(1).getText();
    attribute = OClass.ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
    if(candidate.NULL() != null){
      value = null;
    }else{
      value = candidate.commandAlterClassCWord().getText();
    }
    
    return this;
  }

  /**
   * Execute the ALTER CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (attribute == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final OClassImpl cls = (OClassImpl) getDatabase().getMetadata().getSchema().getClass(className);
    if (cls == null)
      throw new OCommandExecutionException("Source class '" + className + "' not found");

    cls.setInternalAndSave(attribute, value);
    renameCluster();
    return null;
  }

  private void renameCluster() {
    final ODatabaseRecord database = getDatabase();
    if (attribute.equals(OClass.ATTRIBUTES.NAME) && checkClusterRenameOk(database.getStorage().getClusterIdByName(value))) {
      database.command(new OCommandSQL("alter cluster " + className + " name " + value)).execute();
    }
  }

  private boolean checkClusterRenameOk(int clusterId) {
    final ODatabaseRecord database = getDatabase();
    for (OClass clazz : database.getMetadata().getSchema().getClasses()) {
      if (clazz.getName().equals(value))
        continue;
      else if (clazz.getDefaultClusterId() == clusterId || Arrays.asList(clazz.getClusterIds()).contains(clusterId))
        return false;
    }
    return true;
  }

  public String getSyntax() {
    return "ALTER CLASS <class> <attribute-name> <attribute-value>";
  }
    
}
