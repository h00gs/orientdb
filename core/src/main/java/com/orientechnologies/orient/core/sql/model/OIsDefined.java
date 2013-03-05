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
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OIsDefined extends OExpressionWithChildren{


  public OIsDefined(OName exp) {
    this(null,exp);
  }

  public OIsDefined(String alias, OName exp) {
    super(alias,exp);
  }
  
  public OName getExpression(){
    return (OName) children.get(0);
  }
  
  @Override
  protected String thisToString() {
    return "(IsDefined)";
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    OName name = getExpression();
    if(candidate instanceof ODocument){
        final ODocument doc = (ODocument) candidate;
        final String className = doc.getClassName();
        final Object ob = doc.field(name.getName());
        if(ob != null){
            return true;
        }
        if(className != null){
            final OClass clazz = getDatabase().getMetadata().getSchema().getClass(className);
            return clazz.existsProperty(name.getName());
        }
    }
    return false;
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return super.equals(obj);
  }
  
  @Override
  public OIsDefined copy() {
    return new OIsDefined(alias, getExpression());
  }
  
}
