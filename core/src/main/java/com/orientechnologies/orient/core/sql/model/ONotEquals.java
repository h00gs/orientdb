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
import java.util.Collections;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class ONotEquals extends OExpressionWithChildren{
  
  public ONotEquals(OExpression left, OExpression right) {
    this(null,left,right);
  }

  public ONotEquals(String alias, OExpression left, OExpression right) {
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
    return "(NotEquals)";
  }

  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {
    final String className = searchContext.getSource().getTargetClasse();
    if(className == null){
      //no optimisation
      return;
    }
    
    //test is equality match pattern : field = value
    OName fieldName;
    OLiteral literal;
    if(getLeft() instanceof OName && getRight() instanceof OLiteral){
      fieldName = (OName) getLeft();
      literal = (OLiteral) getRight();
    }else if(getLeft() instanceof OLiteral && getRight() instanceof OName){
      fieldName = (OName) getRight();
      literal = (OLiteral) getLeft();
    }else{
      //no optimisation
      return;
    }
    
    //search for an index
    final OClass clazz = getDatabase().getMetadata().getSchema().getClass(className);
    final Set<OIndex<?>> indexes = clazz.getClassInvolvedIndexes(fieldName.getName());
    if(indexes == null || indexes.isEmpty()){
      //no index usable
      return;
    }
    
    for(OIndex index : indexes){
      if(index.getKeyTypes().length != 1){
        continue;
      }
      
      if(OClass.INDEX_TYPE.UNIQUE.toString().equals(index.getType())){
        //found a usable index
        final Collection searchFor = Collections.singleton(literal.evaluate(null, null));
        final Collection<OIdentifiable> ids = index.getValues(searchFor);
        searchResult.setState(OSearchResult.STATE.FILTER);
        searchResult.setExcluded(ids);
        updateStatistic(index);
        return;
      }
    }
    
  }
  
  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    return !OEquals.equals(getLeft(), getRight(), context, candidate);
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
  public ONotEquals copy() {
    return new ONotEquals(alias,getLeft(),getRight());
  }
  
}
