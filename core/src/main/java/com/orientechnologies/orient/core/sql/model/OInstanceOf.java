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
public class OInstanceOf extends OExpressionWithChildren{


  public OInstanceOf(OExpression left, OExpression right) {
    this(null,left,right);
  }

  public OInstanceOf(String alias, OExpression left, OExpression right) {
    super(alias,left,right);
  }
  
  public OExpression getLeft(){
    return children.get(0);
  }
  
  public OExpression getRight(){
    return children.get(1);
  }
  
  @Override
  protected String thisToString() {
    return "(InstanceOf)";
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    Object left = getLeft().evaluate(context, candidate);
    Object right = getRight().evaluate(context, candidate);
      
    OClass cleft = null;
    OClass cright = null;
    
    if(left instanceof ODocument){
        final ODocument doc = (ODocument) left;
        cleft = doc.getSchemaClass();
    }else if(left instanceof String){
        cleft = getDatabase().getMetadata().getSchema().getClass((String)left);
    }
    if(right instanceof ODocument){
        final ODocument doc = (ODocument) right;
        cright = doc.getSchemaClass();
    }else if(right instanceof String){
        cright = getDatabase().getMetadata().getSchema().getClass((String)right);
    }
    
    if(cleft != null && cright != null){
        return cleft.isSubClassOf(cright);
    }else{
        return false;
    }
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
  public OInstanceOf copy() {
    return new OInstanceOf(alias,getLeft(),getRight());
  }
 
}
