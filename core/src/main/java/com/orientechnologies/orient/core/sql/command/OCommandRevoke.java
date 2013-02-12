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
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * SQL REVOKE command: Revoke a privilege to a database role.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandRevoke extends OCommandPermissionAbstract{
  public static final String  KEYWORD_REVOKE = "REVOKE";
  private static final String KEYWORD_FROM   = "FROM";

  
  public OCommandRevoke() {
  }

  public OCommandRevoke parse(final OCommandRequest iRequest) throws OCommandSQLParsingException {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandRevokeContext candidate = getCommand(iRequest, OSQLParser.CommandRevokeContext.class);
    
    privilege = ORole.PERMISSION_NONE;
    resource = null;
    role = null;
    
    parsePrivilege(visitAsString(candidate.reference(0)));
    
    resource = visitAsString(candidate.reference(1));
    
    final String roleName = visitAsString(candidate.reference(2));
    role = getDatabase().getMetadata().getSecurity().getRole(roleName);
    if (role == null){
      throw new OCommandSQLParsingException("Invalid role: " + roleName);
    }
    
    return this;
  }

  /**
   * Execute the command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (role == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not yet been parsed");

    role.revoke(resource, privilege);
    role.save();

    return role;
  }

  public String getSyntax() {
    return "REVOKE <permission> ON <resource> FROM <role>";
  }
  
}
