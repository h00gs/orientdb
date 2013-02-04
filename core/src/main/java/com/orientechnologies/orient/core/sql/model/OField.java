/*
 * Copyright 2013 Orient Technologies.
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.model;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Reference to a document field value.
 * 
 * @author Johann Sorel (Geomatys)
 */
public final class OField extends OExpressionAbstract {

  private final String fieldName;

  public OField(String name) {
    this(name,null);
  }
  
  public OField(String name, String alias) {
    super(alias);
    this.fieldName = name;
  }

  public Object getFieldName() {
    return fieldName;
  }
  
  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    if(candidate instanceof ORID){
      candidate = ((ORID)candidate).getRecord();
    }
    if(candidate instanceof ODocument){
      final ODocument doc = (ODocument) candidate;
      return doc.field(fieldName);
    }
    return null;
  }

  @Override
  public boolean isContextFree() {
    return true;
  }

  @Override
  public boolean isDocumentFree() {
    return false;
  }

  @Override
  public OIndexResult searchIndex(OClass clazz, OSortBy[] sorts) {
    return null;
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  protected String thisToString() {
    return "(Field) "+fieldName;
  }
  
}
