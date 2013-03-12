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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OBetween extends OExpressionWithChildren{
  
  public OBetween(OExpression target, OExpression left, OExpression right) {
    this(null,target,left,right);
  }

  public OBetween(String alias, OExpression target, OExpression left, OExpression right) {
    super(alias,target,left,right);
  }
  
  public OExpression getTarget(){
    return children.get(0);
  }
  
  public OExpression getLeft(){
    return children.get(1);
  }
  
  public OExpression getRight(){
    return children.get(2);
  }
  
  @Override
  protected String thisToString() {
    return "(Between)";
  }

  @Override
  protected void analyzeSearchIndex(final OSearchContext searchContext, final OSearchResult result) {
    
    final String className = searchContext.getSource().getTargetClasse();
    if (className == null) {
      //no optimisation
      return;
    }
    
    if(!(getLeft() instanceof OLiteral && getRight() instanceof OLiteral)){
        //no optimisation
        return;
    }
    
    Map.Entry<List<OName>, OExpression> stack = ORangedFilter.toStackPath(getTarget(),OExpression.INCLUDE);
    if (stack == null) return;

    final List<OName> path = stack.getKey();
    OClass clazz = getDatabase().getMetadata().getSchema().getClass(className);
    final Map.Entry<List<OIndex>,OClass> indexUnfold = OPath.unfoldIndexes(path, clazz);
    if (indexUnfold == null) return;
    clazz = indexUnfold.getValue();
    final OName fieldName = path.get(path.size()-1);
    
    //search for indexes
    final Set<OIndex<?>> indexes = clazz.getClassInvolvedIndexes(fieldName.getName());
    if(indexes == null || indexes.isEmpty()){
      //no index usable
      return;
    }
    
    Object min = getLeft().evaluate(null, null);
    Object max = getRight().evaluate(null, null);
    
    boolean found = false;
    for(OIndex index : indexes){
      if(index.getKeyTypes().length != 1){
        continue;
      }
      
      //found a usable index
      final Collection<OIdentifiable> ids = index.getValuesBetween(min,max);
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
    final Object objTarget = getTarget().evaluate(context, candidate);
    final Object objLeft = getLeft().evaluate(context, candidate);
    final Object objRight = getRight().evaluate(context, candidate);
    
    final Integer minRange = OInferior.compare(objTarget,objLeft);
    if(minRange == null || minRange < 0){
      return false;
    }
    final Integer maxRange = OInferior.compare(objTarget,objRight);
    if(maxRange == null || maxRange > 0){
      return false;
    }
    
    return true ;
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
  public OBetween copy() {
    return new OBetween(alias, getTarget(),getLeft(),getRight());
  }
  
}
