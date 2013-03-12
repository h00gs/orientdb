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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OIn extends OExpressionWithChildren{
  
  public OIn(OExpression left, OExpression right) {
    this(null,left,right);
  }

  public OIn(String alias, OExpression left, OExpression right) {
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
    return "(IN)";
  }

  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {
    
    final String className = searchContext.getSource().getTargetClasse();
    if (className == null) {
      //no optimisation
      return;
    }
    
    Map.Entry<List<OName>, OExpression> stack = ORangedFilter.toStackPath(getLeft(),getRight());
    if (stack == null) return;

    final List<OName> path = stack.getKey();
    OClass clazz = getDatabase().getMetadata().getSchema().getClass(className);
    final Map.Entry<List<OIndex>,OClass> indexUnfold = OPath.unfoldIndexes(path, clazz);
    if (indexUnfold == null) return;
    clazz = indexUnfold.getValue();
    final OName fieldName = path.get(path.size()-1);
    final OExpression fieldValue = stack.getValue();
    
    //search for indexes
    final Set<OIndex<?>> indexes = clazz.getClassInvolvedIndexes(fieldName.getName());
    if(indexes == null || indexes.isEmpty()){
      //no index usable
      return;
    }
        
    boolean found = false;
    for(OIndex index : indexes){
      if(index.getKeyTypes().length != 1){
        continue;
      }
      
      final Object val = fieldValue.evaluate(null, null);
      final Collection col;
      if(val instanceof Collection){
          col = (Collection) val;
      }else{
          col = Collections.singleton(val);
      }
      
      //found a usable index
      final Collection<OIdentifiable> ids = index.getValues(col);
      searchResult.setState(OSearchResult.STATE.FILTER);
      searchResult.setIncluded(ids);
      updateStatistic(index);
      found = true;
      break;
    }
    
    if (!found) {
      //could not find a proper index
      return;
    }
    
    OPath.foldIndexes(this, indexUnfold.getKey(), searchResult);    
  }
  
  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    final Object left = getLeft().evaluate(context, candidate);
    
    final Object[] rights;
    if(getRight() instanceof OCollection){
        final List<OExpression> col = ((OCollection) getRight()).getChildren();
        rights = new Object[col.size()];
        for(int i=0;i<rights.length;i++){
            rights[i] = col.get(i).evaluate(context, candidate);
        }
    }else{
        Object r = getRight().evaluate(context, candidate);
        if(r instanceof Collection){
            rights = ((Collection)r).toArray();
        }else{
            rights = new Object[]{r};
        }
    }
    
    if(left instanceof Collection){
        //act as if it was : any value IN <right>
        final Collection col = (Collection) left;
        for(Object l : col){
            if(evaluateOne(l, rights)){
                return true;
            }
        }
        return false;
    }else{
        return evaluateOne(left, rights);
    }
  }
  
  private boolean evaluateOne(Object left, Object[] rights){
      for(Object r : rights){
          if(OEquals.equals(left, r)){
              return true;
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
  public OIn copy() {
    return new OIn(alias, getLeft(),getRight());
  }
  
}
