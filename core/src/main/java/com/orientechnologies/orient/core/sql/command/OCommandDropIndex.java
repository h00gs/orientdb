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
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;

/**
 * SQL REMOVE INDEX command: Remove an index
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandDropIndex extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  
  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_INDEX = "INDEX";
  private String name;
  
  public OCommandDropIndex() {
  }

  public OCommandDropIndex parse(final OCommandRequest iRequest) throws OCommandSQLParsingException {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandDropIndexContext candidate = SQLGrammarUtils
            .getCommand(iRequest, OSQLParser.CommandDropIndexContext.class);
    
    name = candidate.word().getText();
    return this;
  }
  
  /**
   * Execute the REMOVE INDEX.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (name == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    getDatabase().getMetadata().getIndexManager().dropIndex(name);
    return null;
  }

  @Override
  public String getSyntax() {
    return "DROP INDEX <index-name>|<class>.<property>";
  }
  
}
