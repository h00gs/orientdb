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
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * SQL REBUILD INDEX command: rebuild an index
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Johann Sorel (Geomatys)
 */
public class OCommandRebuildIndex extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  
  public static final String KEYWORD_REBUILD = "REBUILD";
  public static final String KEYWORD_INDEX   = "INDEX";
  private String name;
  
  public OCommandRebuildIndex() {
  }

  public OCommandRebuildIndex parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandRebuildIndexContext candidate = getCommand(iRequest, OSQLParser.CommandRebuildIndexContext.class);    
    name = visitAsString(candidate.reference());
    
    return this;
  }

  /**
   * Execute the REMOVE INDEX.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (name == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    if (name.equals("*")) {
      long totalIndexed = 0;
      for (OIndex<?> idx : database.getMetadata().getIndexManager().getIndexes()) {
        if (idx.isAutomatic())
          totalIndexed += idx.rebuild();
      }

      return totalIndexed;

    } else {
      final OIndex<?> idx = database.getMetadata().getIndexManager().getIndex(name);
      if (idx == null)
        throw new OCommandExecutionException("Index '" + name + "' not found");

      if (!idx.isAutomatic())
        throw new OCommandExecutionException("Cannot rebuild index '" + name
            + "' because it's manual and there aren't indications of what to index");

      return idx.rebuild();
    }
  }

  @Override
  public String getSyntax() {
    return "REBUILD INDEX <index-name>";
  }
}
