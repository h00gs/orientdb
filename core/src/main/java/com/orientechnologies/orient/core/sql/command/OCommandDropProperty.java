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
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.util.OCaseIncentiveComparator;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandDropProperty extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  
  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_PROPERTY = "PROPERTY";

  private String className;
  private String fieldName;
  private boolean force = false;
  
  public OCommandDropProperty() {
  }

  public OCommandDropProperty parse(final OCommandRequest iRequest) throws OCommandSQLParsingException {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandDropPropertyContext candidate = getCommand(iRequest, OSQLParser.CommandDropPropertyContext.class);
    
    className = visitAsString(candidate.reference(0));
    fieldName = visitAsString(candidate.reference(1));
    force = (candidate.FORCE() != null);
    
    return this;
  }
    
  
  /**
   * Execute the CREATE PROPERTY.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (fieldName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not yet been parsed");

    final ODatabaseRecord database = getDatabase();
    final OClassImpl sourceClass = (OClassImpl) database.getMetadata().getSchema().getClass(className);
    if (sourceClass == null)
      throw new OCommandExecutionException("Source class '" + className + "' not found");

    final List<OIndex<?>> indexes = relatedIndexes(fieldName);
    if (!indexes.isEmpty()) {
      if (force) {
        dropRelatedIndexes(indexes);
      } else {
        final StringBuilder indexNames = new StringBuilder();

        boolean first = true;
        for (final OIndex<?> index : sourceClass.getClassInvolvedIndexes(fieldName)) {
          if (!first) {
            indexNames.append(", ");
          } else {
            first = false;
          }
          indexNames.append(index.getName());
        }

        throw new OCommandExecutionException("Property used in indexes (" + indexNames.toString()
            + "). Please drop these indexes before removing property or use FORCE parameter.");
      }
    }

    // REMOVE THE PROPERTY
    sourceClass.dropPropertyInternal(fieldName);
    sourceClass.saveInternal();

    return null;
  }

  private void dropRelatedIndexes(final List<OIndex<?>> indexes) {
    final ODatabaseRecord database = getDatabase();
    for (final OIndex<?> index : indexes) {
      database.command(new OCommandSQL("DROP INDEX " + index.getName())).execute();
    }
  }

  private List<OIndex<?>> relatedIndexes(final String fieldName) {
    final List<OIndex<?>> result = new ArrayList<OIndex<?>>();

    final ODatabaseRecord database = getDatabase();
    for (final OIndex<?> oIndex : database.getMetadata().getIndexManager().getClassIndexes(className)) {
      if (OCollections.indexOf(oIndex.getDefinition().getFields(), fieldName, new OCaseIncentiveComparator()) > -1) {
        result.add(oIndex);
      }
    }

    return result;
  }

  @Override
  public String getSyntax() {
    return "DROP PROPERTY <class>.<property>";
  }
}
