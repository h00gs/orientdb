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

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;

/**
 * SQL GRANT command: Grant a privilege to a database role.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandGrant extends OCommandPermissionAbstract {
  public static final String  KEYWORD_GRANT = "GRANT";
  private static final String KEYWORD_TO    = "TO";
  
  public OCommandGrant() {
  }

  public OCommandGrant parse(final OCommandRequest iRequest) throws OCommandSQLParsingException {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandGrantContext candidate = SQLGrammarUtils
            .getCommand(iRequest, OSQLParser.CommandGrantContext.class);
    
    privilege = ORole.PERMISSION_NONE;
    resource = null;
    role = null;
    
    parsePrivilege(candidate.word(0).getText());
    
    resource = candidate.word(1).getText();
    
    final String roleName = candidate.word(2).toString();
    role = getDatabase().getMetadata().getSecurity().getRole(roleName);
    if (role == null){
      throw new OCommandSQLParsingException("Invalid role: " + roleName);
    }
    
    return this;
  }
  
  /**
   * Execute the GRANT.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (role == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    role.grant(resource, privilege);
    role.save();

    return role;
  }

  public String getSyntax() {
    return "GRANT <permission> ON <resource> TO <role>";
  }
  
}
