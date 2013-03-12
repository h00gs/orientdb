/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.common.collection.OAlwaysGreaterKey;
import com.orientechnologies.common.collection.OAlwaysLessKey;
import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OName;
import com.orientechnologies.orient.core.sql.model.OPath;
import com.orientechnologies.orient.core.sql.model.ORangedFilter;
import com.orientechnologies.orient.core.sql.model.OSearchContext;
import com.orientechnologies.orient.core.sql.model.OSearchResult;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import java.util.Map;
import java.util.Set;

/**
 * CONTAINS VALUE operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLMethodContainsValue extends OSQLMethod {

  public static final String NAME = "containsvalue";

  public OSQLMethodContainsValue() {
    super(NAME, 1);
  }
  
  public OSQLMethodContainsValue(OExpression left, OExpression right) {
    super(NAME, 1);
    children.add(left);
    children.add(right);
  }

  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {
      
    final String className = searchContext.getSource().getTargetClasse();
    if (className == null) {
      //no optimisation
      return;
    }
    
    if(children.size() != 2 || !(children.get(1) instanceof OLiteral) ){
      //no optimisation
      return;
    }
    
    Map.Entry<List<OName>, OExpression> stack = ORangedFilter.toStackPath(children.get(0),children.get(1));
    if (stack == null) return;

    final List<OName> path = stack.getKey();
    OClass clazz = getDatabase().getMetadata().getSchema().getClass(className);
    final Map.Entry<List<OIndex>,OClass> indexUnfold = OPath.unfoldIndexes(path, clazz);
    if (indexUnfold == null) return;
    clazz = indexUnfold.getValue();
    final OName expfieldName = path.get(path.size()-1);
    final String fieldName = expfieldName.getName();
    final Object fieldValue = stack.getValue().evaluate(null, null);
    
    final Set<OIndex<?>> indexes = clazz.getClassInvolvedIndexes(fieldName);
    if(indexes == null || indexes.isEmpty()){
      //no index usable
      return;
    }
    
    boolean found = false;
    for(OIndex index : indexes){
      final OIndexDefinition definition = index.getDefinition();
      
      if(definition instanceof OPropertyMapIndexDefinition){
        final OPropertyMapIndexDefinition mdef = (OPropertyMapIndexDefinition) definition;
        if(mdef.getIndexBy() != OPropertyMapIndexDefinition.INDEX_BY.VALUE){
          continue;
        }

        //found a usable index
        final Collection searchFor = Collections.singleton(fieldValue);
        final Collection<OIdentifiable> ids = index.getValues(searchFor);
        searchResult.setState(OSearchResult.STATE.FILTER);
        searchResult.setIncluded(ids);
        updateStatistic(index);
        found = true;
        break;
      }
      
      //may be a composite index
      final List<String> keyFields = definition.getFieldsToIndex();
      final Object[] fkmin = new Object[keyFields.size()];
      final Object[] fkmax = new Object[keyFields.size()];
      boolean fieldfound = false;
      for(int i=0;i<fkmax.length;i++){
        final String s = keyFields.get(i);
        if(s.startsWith(fieldName) && s.endsWith("by value")){
          fieldfound = true;
          fkmin[i] = fieldValue;
          fkmax[i] = fieldValue;
        }else{
          fkmin[i] = new OAlwaysLessKey();
          fkmax[i] = new OAlwaysGreaterKey();
        }
      }
      if(!fieldfound) continue;

      //found a usable index
      final OCompositeKey minkey = new OCompositeKey(fkmin);
      final OCompositeKey maxkey = new OCompositeKey(fkmax);
      final Collection<OIdentifiable> ids = index.getValuesBetween(minkey, maxkey);
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
    final Object iLeft = children.get(0).evaluate(context,candidate);    
    final OExpression right = children.get(1);
    
    if (!(iLeft instanceof Map<?, ?>)) {
      return false;
    }
    final Map<?,?> map = (Map<String, ?>) iLeft;
    
    if(right instanceof OLiteral){
        final Object iRight = right.evaluate(context,candidate);
        return map.containsValue(iRight);
    }else{
        //it's not a contain but a filter test
        for(Object o : map.values()){
            if(Boolean.TRUE.equals(right.evaluate(context, o))){
                return true;
            }
        }
        return false;
    }
  }

  @Override
  public OSQLMethodContainsValue copy() {
    final OSQLMethodContainsValue cp = new OSQLMethodContainsValue();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }

}
