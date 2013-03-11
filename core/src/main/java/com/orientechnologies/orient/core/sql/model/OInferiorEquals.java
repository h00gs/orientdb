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
public class OInferiorEquals extends OBinaryFilter{
  
  public OInferiorEquals(OExpression left, OExpression right) {
    this(null,left,right);
  }

  public OInferiorEquals(String alias, OExpression left, OExpression right) {
    super(alias,left,right);
  }
    
  @Override
  protected String thisToString() {
    return "(<=)";
  }

  @Override
  protected boolean analyzeSearchIndex(OSearchContext searchContext, OSearchResult result, 
        OClass clazz, OName fieldName, OExpression fieldValue) {
      
    final boolean under = (getLeft() instanceof OName || getLeft() instanceof OPath);
    
    //search for an index
    final Set<OIndex<?>> indexes = clazz.getClassInvolvedIndexes(fieldName.getName());
    if(indexes == null || indexes.isEmpty()){
      //no index usable
      return false;
    }
    
    boolean found = false;
    for(OIndex index : indexes){
      if(index.getKeyTypes().length != 1){
        continue;
      }
      
      //found a usable index
      final Object key = fieldValue.evaluate(null, null);
      final Collection<OIdentifiable> ids;
      if(under){
          ids = index.getValuesMinor(key, true);
      }else{
          ids = index.getValuesMajor(key, true);
      }
      searchResult.setState(OSearchResult.STATE.FILTER);
      searchResult.setIncluded(ids);
      updateStatistic(index);
      found = true;
      break;
    }
    return found;
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
