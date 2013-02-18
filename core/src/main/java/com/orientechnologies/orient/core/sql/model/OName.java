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
public final class OName extends OExpressionAbstract {

  private final String name;

  public OName(String name) {
    this(null,name);
  }
  
  public OName(String alias, String name) {
    super(alias);
    //by default name expression has the same alias
    if(alias == null){
      setAlias(name);
    }
    this.name = name;
  }

  public String getName() {
    return name;
  }
  
  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    if(candidate instanceof ORID){
      candidate = ((ORID)candidate).getRecord();
    }
    if(candidate instanceof ODocument){
      final ODocument doc = (ODocument) candidate;
      return doc.field(name);
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
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  protected String thisToString() {
    return "(Name) "+name;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 29 * hash + (this.name != null ? this.name.hashCode() : 0);
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
    final OName other = (OName) obj;
    if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
      return false;
    }
    return true;
  }
  
  @Override
  public OName copy() {
    return new OName(alias,getName());
  }
  
}
