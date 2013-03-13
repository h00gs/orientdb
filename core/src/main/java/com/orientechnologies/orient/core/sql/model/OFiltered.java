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

  public OFiltered(OExpression source) {
    super(null,source);
    if(source.getAlias() != null){
        setAlias(source.getAlias());
    }
  }

  public OFiltered(OExpression source, OExpression filter) {
    super(null,source,filter);
    if(source.getAlias() != null){
        setAlias(source.getAlias());
    }
  }
  
  public OFiltered(String alias, OExpression source, OExpression filter) {
    super(alias,source,filter);
    if(alias == null && source.getAlias() != null){
        setAlias(source.getAlias());
    }
  }
  
  public OFiltered(String alias, OExpression source, OExpression r1, OExpression r2) {
    super(alias,source,r1,r2);
    if(alias == null && source!= null && source.getAlias() != null){
        setAlias(source.getAlias());
    }
  }
  
  public OFiltered(String alias, OExpression ... exps) {
    super(alias,exps);
    if(alias == null && exps != null && exps.length>0){
        setAlias(exps[0].getAlias());
    }
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
    OExpression filter = getFilter();
    
    if(left instanceof ODocument && getFilter() instanceof OLiteral){
        //it's not a filter but a document accessor
        final ODocument doc = (ODocument) left;
        final String fieldName = String.valueOf(filter.evaluate(context, candidate));
        return doc.field(fieldName);
    }else if(left instanceof ODocument && (filter instanceof OName || filter instanceof OPath) ){
        //it's not a filter but a document accessor
        return filter.evaluate(context, candidate);
    }else if(left instanceof Collection && (filter instanceof OName || filter instanceof OPath) ){
        //it's not a filter but a document accessor
        final List result = new ArrayList<Object>();
        for(Object o : ((Collection)left)){
          final Object obj = filter.evaluate(context, o);
          if(obj != null){
            result.add(obj);
          }
        }
        if(result.size() == 1){
          return result.get(0);
        }else{
          return result;
        }
    }else if(left instanceof Map && filter instanceof OLiteral){
        //it's not a filter but a Map accessor
        return ((Map)left).get(((OLiteral) filter).evaluateNow(context,candidate));
    }else if(getChildren().size()==3){
        //it's not a filter but a subcollection clip
        if(!(left instanceof Collection)){
            //invalid element
            return null;
        }
        final List col = new ArrayList((Collection)left);
        final Number startIndex = (Number)children.get(1).evaluate(context, candidate);
        final Number endIndex = (Number)children.get(2).evaluate(context, candidate);
        return col.subList(startIndex.intValue(), endIndex.intValue()+1);//+1 because edges are inclusive
        
    }else if(left instanceof Collection && filter instanceof OLiteral){
        //it's not a filter but a List accessor
        left = new ArrayList((Collection)left);
        final List col = (List) left;
        final Number index = (Number)children.get(1).evaluate(context, candidate);
        return col.get(index.intValue());
    }

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
    }else{
        test(context,left,result);
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
    
    if(Boolean.TRUE.equals(getFilter().evaluate(context, candidate))){
      //single valid element
      result.add(candidate);
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
    return "(Filtered/Accessor) ";
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
    final OFiltered cp = new OFiltered(getAlias());
    cp.getChildren().addAll(getChildren());
    return cp;
  }
  
}
