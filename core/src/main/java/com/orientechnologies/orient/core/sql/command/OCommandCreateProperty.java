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
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import java.util.Locale;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 */
public class OCommandCreateProperty extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  
  public static final String KEYWORD_CREATE   = "CREATE";
  public static final String KEYWORD_PROPERTY = "PROPERTY";

  private String className;
  private String fieldName;
  private OType type;
  private String linked;
  
  public OCommandCreateProperty() {
  }

  public OCommandCreateProperty parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandCreatePropertyContext candidate = SQLGrammarUtils
            .getCommand(iRequest, OSQLParser.CommandCreatePropertyContext.class);
    
    int i= 0;
    className = candidate.word(i++).getText();
    fieldName = candidate.word(i++).getText();    
    type = OType.valueOf(candidate.word(i++).getText().toUpperCase());
    
    if(candidate.word().size()>3){
      linked = candidate.word(i++).getText().toUpperCase();
    }
    
    return this;
  }

  /**
   * Execute the CREATE PROPERTY.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (type == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    final OClassImpl sourceClass = (OClassImpl) database.getMetadata().getSchema().getClass(className);
    if (sourceClass == null)
      throw new OCommandExecutionException("Source class '" + className + "' not found");

    OPropertyImpl prop = (OPropertyImpl) sourceClass.getProperty(fieldName);
    if (prop != null)
      throw new OCommandExecutionException("Property '" + className + "." + fieldName
          + "' already exists. Remove it before to retry.");

    // CREATE THE PROPERTY
    OClass linkedClass = null;
    OType linkedType = null;
    if (linked != null) {
      // FIRST SEARCH BETWEEN CLASSES
      linkedClass = database.getMetadata().getSchema().getClass(linked);

      if (linkedClass == null)
        // NOT FOUND: SEARCH BETWEEN TYPES
        linkedType = OType.valueOf(linked.toUpperCase(Locale.ENGLISH));
    }

    // CREATE IT LOCALLY
    prop = sourceClass.addPropertyInternal(fieldName, type, linkedType, linkedClass);
    sourceClass.saveInternal();

    return sourceClass.properties().size();
  }

  @Override
  public String getSyntax() {
    return "CREATE PROPERTY <class>.<property> <type> [<linked-type>|<linked-class>]";
  }
  
}
