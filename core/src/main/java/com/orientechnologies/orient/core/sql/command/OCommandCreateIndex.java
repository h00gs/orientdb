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
import java.util.Arrays;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionFactory;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * SQL CREATE INDEX command: Create a new index against a property.
 * <p/>
 * <p>
 * Supports following grammar: <br/>
 * "CREATE" "INDEX" &lt;indexName&gt; ["ON" &lt;className&gt; "(" &lt;propName&gt; ("," &lt;propName&gt;)* ")"] &lt;indexType&gt;
 * [&lt;keyType&gt; ("," &lt;keyType&gt;)*]
 * </p>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandCreateIndex extends OCommandAbstract implements OCommandDistributedReplicateRequest{
  
  public static final String KEYWORD_CREATE = "CREATE";
  public static final String KEYWORD_INDEX  = "INDEX";
  public static final String KEYWORD_ON     = "ON";

  private String             indexName;
  private OClass             oClass;
  private String[]           fields;
  private OClass.INDEX_TYPE  indexType;
  private OType[]            keyTypes;
  private byte               serializerKeyId;
  
  public OCommandCreateIndex() {
  }

  public OCommandCreateIndex parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandCreateIndexContext candidate = getCommand(iRequest, OSQLParser.CommandCreateIndexContext.class);
    
    int i=0;
    indexName = visitAsString(candidate.reference(i++));
    
    if(candidate.indexOn()!= null){
      final OSQLParser.IndexOnContext ctx = candidate.indexOn();
      final List<OSQLParser.ReferenceContext> words = ctx.reference();
      oClass = findClass(words.get(0).getText());
      fields = new String[words.size()-1];
      for(int k=1;k<fields.length;k++){
        fields[k-1] = visitAsString(words.get(k));
      }
    }
    
    indexType = OClass.INDEX_TYPE.valueOf(visitAsString(candidate.reference(i++)));
    
    if(candidate.NULL() != null){
      //do nothing
    }else if(candidate.RUNTIME() != null){
      serializerKeyId = Byte.parseByte(candidate.INT().getText());
    }else{
      final List<OType> keyTypes = new ArrayList<OType>();
      for(;i<candidate.reference().size();i++){
        final String text = visitAsString(candidate.reference(i));
        keyTypes.add(OType.valueOf(text));
      }
      this.keyTypes = keyTypes.toArray(new OType[0]);
      if (this.fields.length != this.keyTypes.length) {
          throw new OCommandSQLParsingException("Count of fields doesn't match with count of property types. " + "Fields: "
              + Arrays.toString(this.fields) + "; Types: " + Arrays.toString(this.keyTypes));
        }
    }
    return this;
  }

  private OClass findClass(String part) {
    return getDatabase().getMetadata().getSchema().getClass(part);
  }

  /**
   * Execute the CREATE INDEX.
   */
  @SuppressWarnings("rawtypes")
  public Object execute(final Map<Object, Object> iArgs) {
    if (indexName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    final OIndex<?> idx;
    if (fields == null || fields.length == 0) {
      if (keyTypes != null)
        idx = database.getMetadata().getIndexManager()
            .createIndex(indexName, indexType.toString(), new OSimpleKeyIndexDefinition(keyTypes), null, null);
      else if (serializerKeyId != 0) {
        idx = database.getMetadata().getIndexManager()
            .createIndex(indexName, indexType.toString(), new ORuntimeKeyIndexDefinition(serializerKeyId), null, null);
      } else
        idx = database.getMetadata().getIndexManager().createIndex(indexName, indexType.toString(), null, null, null);
    } else {
      if (keyTypes == null || keyTypes.length == 0) {
        idx = oClass.createIndex(indexName, indexType, fields);
      } else {
        final OIndexDefinition idxDef = OIndexDefinitionFactory.createIndexDefinition(oClass, Arrays.asList(fields),
            Arrays.asList(keyTypes));

        idx = database.getMetadata().getIndexManager()
            .createIndex(indexName, indexType.name(), idxDef, oClass.getPolymorphicClusterIds(), null);
      }
    }

    if (idx != null)
      return idx.getSize();

    return null;
  }

  
  private void checkMapIndexSpecifier(final String fieldName, final String text, final int pos) {
    String[] fieldNameParts = fieldName.split("\\s+");
    if (fieldNameParts.length == 1)
      return;

    if (fieldNameParts.length == 3) {
      if ("by".equals(fieldNameParts[1].toLowerCase())) {
        try {
          OPropertyMapIndexDefinition.INDEX_BY.valueOf(fieldNameParts[2].toUpperCase());
        } catch (IllegalArgumentException iae) {
          throw new OCommandSQLParsingException("Illegal field name format, should be '<property> [by key|value]' but was '"
              + fieldName + "'", text, pos);
        }
        return;
      }
      throw new OCommandSQLParsingException("Illegal field name format, should be '<property> [by key|value]' but was '"
          + fieldName + "'", text, pos);
    }

    throw new OCommandSQLParsingException("Illegal field name format, should be '<property> [by key|value]' but was '" + fieldName
        + "'", text, pos);
  }
  
  @Override
  public String getSyntax() {
    return "CREATE INDEX <name> [ON <class-name> (prop-names)] <type> [<key-type>]";
  }
  
}
