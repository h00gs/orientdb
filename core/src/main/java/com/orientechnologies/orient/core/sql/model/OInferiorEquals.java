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
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OInferiorEquals extends OExpressionWithChildren{
  
  public OInferiorEquals(OExpression left, OExpression right) {
    this(null,left,right);
  }

  public OInferiorEquals(String alias, OExpression left, OExpression right) {
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
    return "(<=)";
  }

  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {
    final String className = searchContext.getSource().getTargetClasse();
    if(className == null){
      //no optimisation
      return;
    }
    
    //test is inferior match pattern : field <= value
    final boolean under;
    OName fieldName;
    OLiteral literal;
    if(getLeft() instanceof OName && getRight() instanceof OLiteral){
      fieldName = (OName) getLeft();
      literal = (OLiteral) getRight();
      under = true;
    }else if(getLeft() instanceof OLiteral && getRight() instanceof OName){
      fieldName = (OName) getRight();
      literal = (OLiteral) getLeft();
      under = false;
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
      
      final Object key = literal.evaluate(null, null);
      
      //found a usable index
      final Collection<OIdentifiable> ids;
      if(under){
          ids = index.getValuesMinor(key, true);
      }else{
          ids = index.getValuesMajor(key, true);
      }
      searchResult.setState(OSearchResult.STATE.FILTER);
      searchResult.setIncluded(ids);
      updateStatistic(index);
      return;
    }
    
  }
  
  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    final Integer v = OInferior.compare(getLeft(),getRight(),context,candidate);
    return (v == null) ? false : (v <= 0) ;
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
  public OInferiorEquals copy() {
    return new OInferiorEquals(alias, getLeft(),getRight());
  }
  
}
