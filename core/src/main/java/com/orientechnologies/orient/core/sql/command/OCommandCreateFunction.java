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
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import java.util.Locale;

/**
 * SQL CREATE FUNCTION command.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 */
public class OCommandCreateFunction extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  public static final String NAME       = "CREATE FUNCTION";
  private String             name;
  private String             code;
  private String             language;
  private boolean            idempotent = false;
  
  public OCommandCreateFunction() {
  }

  public OCommandCreateFunction parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandCreateFunctionContext candidate = SQLGrammarUtils
            .getCommand(iRequest, OSQLParser.CommandCreateFunctionContext.class);
    
    int i=0;
    name = candidate.word(i++).getText();
    code = SQLGrammarUtils.visitText(candidate.TEXT());
    
    if(candidate.IDEMPOTENT() != null){
      idempotent = "TRUE".equalsIgnoreCase(candidate.word(i++).getText());
    }
    
    if(candidate.LANGUAGE() != null){
      language = candidate.word(i++).getText();
    }
    
    return this;
  }

  
  /**
   * Execute the command and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (name == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    ODatabaseRecord database = getDatabase();
    final OFunction f = database.getMetadata().getFunctionLibrary().createFunction(name);
    f.setCode(code);
    f.setIdempotent(idempotent);
    if (language != null)
      f.setLanguage(language);

    return f.getId();
  }

  @Override
  public String getSyntax() {
    return "CREATE FUNCTION <name> <code> [IDEMPOTENT true|false] [LANGUAGE <language>]";
  }

}
