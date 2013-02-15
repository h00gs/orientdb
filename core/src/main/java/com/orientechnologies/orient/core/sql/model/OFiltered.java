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
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Reference to a document field value.
 * 
 * @author Johann Sorel (Geomatys)
 */
public final class OFiltered extends OExpressionWithChildren {


  public OFiltered(OExpression source, OExpression filter) {
    this(null,source,filter);
  }
  
  public OFiltered(String alias, OExpression source, OExpression filter) {
    super(alias,source,filter);
  }

  public OExpression getSource() {
    return getChildren().get(0);
  }
  
  public OExpression getFilter(){
    return getChildren().get(1);
  }
  
  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    Object left = getSource().evaluate(context, candidate);
    
    final List<ODocument> result = new ArrayList<ODocument>();
    
    test(context,left,result);
    
    if(left instanceof Map){
      left = ((Map)left).values();
    }
    
    if(left instanceof Collection){
      final Collection col = (Collection) left;
      for(Object c : col){
        test(context, c, result);
      }
    }
    
    final int size = result.size();
    if(size == 0){
      return null;
    }else if(size == 1){
      return result.get(0);
    }else{
      return result;
    }
  }

  private void test(OCommandContext context, Object candidate, List result){
    if(candidate == null) return;
    
    if(candidate instanceof ORID){
      candidate = ((ORID)candidate).getRecord();
    }
  
    if(candidate instanceof ODocument){
      final ODocument doc = (ODocument) candidate;
      if(Boolean.TRUE.equals(getFilter().evaluate(context, candidate))){
        //single valid element
        result.add(doc);
      }
    }
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
    return "(Filtered) ";
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
  public OFiltered copy() {
    return new OFiltered(alias,getSource(),getFilter());
  }
  
}
