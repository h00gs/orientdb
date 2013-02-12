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
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OFindReferenceHelper;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * FIND REFERENCES command: Finds references to records in all or part of database
 * 
 * @author Luca Molino
 * @author Johann Sorel (Geomatys)
 */
public class OCommandFindReferences extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  public static final String KEYWORD_FIND       = "FIND";
  public static final String KEYWORD_REFERENCES = "REFERENCES";

  private OSQLParser.SourceContext source;
  private String classList;

  
  public OCommandFindReferences() {
  }

  public OCommandFindReferences parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandFindReferencesContext candidate = getCommand(iRequest, OSQLParser.CommandFindReferencesContext.class);
    
    source = candidate.source();
    classList = candidate.cword().getText();
    
    return this;
  }

  /**
   * Execute the FIND REFERENCES.
   */
  public Object execute(final Map<Object, Object> iArgs) {    
    final Set<ORID> recordIds = new HashSet<ORID>();
    recordIds.addAll(visit(source));    
    return OFindReferenceHelper.findReferences(recordIds, classList);
  }

  @Override
  public String getSyntax() {
    return "FIND REFERENCES <rid|<sub-query>> [class-list]";
  }
  
}
