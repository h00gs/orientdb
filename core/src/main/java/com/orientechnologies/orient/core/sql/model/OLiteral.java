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
import com.orientechnologies.orient.core.metadata.schema.OClass;

/**
 * Literal value in SQL.
 * 
 * @author Johann Sorel (Geomatys)
 */
public final class OLiteral extends OExpressionAbstract{

  private final Object value;

  public OLiteral(Object value) {
    this(null,value);
  }
  
  public OLiteral(String alias, Object value) {
    super(alias);
    this.value = value;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    return value;
  }

  @Override
  public boolean isContextFree() {
    return true;
  }

  @Override
  public boolean isDocumentFree() {
    return true;
  }

  @Override
  public OSearchResult searchIndex(OSearchContext searchContext) {
    return null;
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this,data);
  }
  
  @Override
  protected String thisToString() {
    return "(Literal) "+value;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 41 * hash + (this.value != null ? this.value.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final OLiteral other = (OLiteral) obj;
    if (this.value != other.value && (this.value == null || !this.value.equals(other.value))) {
      return false;
    }
    return true;
  }

  @Override
  public OLiteral copy() {
    return new OLiteral(alias, value);
  }
  
  
}
