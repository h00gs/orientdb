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
package com.orientechnologies.orient.core.sql.model.reflect;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.model.OExpressionAbstract;
import com.orientechnologies.orient.core.sql.model.OExpressionVisitor;

/**
 * Reference to a document orid.
 * 
 * @author Johann Sorel (Geomatys)
 */
public final class OExpressionORID extends OExpressionAbstract {

  public OExpressionORID() {
    this(null);
  }
  
  public OExpressionORID(String alias) {
    super(alias);
  }
  
  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    if(candidate instanceof ORID){
      candidate = ((ORID)candidate).getRecord();
    }
    if(candidate instanceof ODocument){
      final ODocument doc = (ODocument) candidate;
      return doc.getIdentity().toString();
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
    return "(@Rid) ";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return true;
  }
  
  @Override
  public OExpressionORID copy() {
    return new OExpressionORID(alias);
  }
  
}
