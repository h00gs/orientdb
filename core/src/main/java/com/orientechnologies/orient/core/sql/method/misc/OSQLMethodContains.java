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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.model.OEquals;
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
 * CONTAINS operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLMethodContains extends OSQLMethod {

  public static final String NAME = "contains";

  public OSQLMethodContains() {
    super(NAME,1);
  }
  
  public OSQLMethodContains(OExpression left, OExpression right) {
    super(NAME,1);
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
    final OName fieldName = path.get(path.size()-1);
    final OExpression fieldValue = stack.getValue();
    
    final Set<OIndex<?>> indexes = clazz.getClassInvolvedIndexes(fieldName.getName());
    if(indexes == null || indexes.isEmpty()){
      //no index usable
      return;
    }
    
    boolean found = false;
    for(OIndex index : indexes){
      if(index.getKeyTypes().length == 1){
        //perfect match index
        //found a usable index
        final Object key = fieldValue.evaluate(null, null);
        final Collection searchFor = Collections.singleton(key);
        final Collection<OIdentifiable> ids = index.getValues(searchFor);
        searchResult.setState(OSearchResult.STATE.FILTER);
        searchResult.setIncluded(ids);
        updateStatistic(index);
        found = true;
        break;
      }else{
        // composite key index
        final List<String> fields = index.getDefinition().getFields();
        if(fields.get(0).equalsIgnoreCase(fieldName.getName())){
          //we can use this index by only setting the last key element
          final Object fkv = fieldValue.evaluate(null, null);
          final Object[] fkmin = new Object[fields.size()];
          final Object[] fkmax = new Object[fields.size()];
          fkmin[0] = fkv;
          fkmax[0] = fkv;
          for(int i=1;i<fkmax.length;i++){
            fkmin[i] = new OAlwaysLessKey();
            fkmax[i] = new OAlwaysGreaterKey();
          }
          final OCompositeKey minkey = new OCompositeKey(fkmin);
          final OCompositeKey maxkey = new OCompositeKey(fkmax);
          final Collection<OIdentifiable> ids = index.getValuesBetween(minkey, true, maxkey, true);
          searchResult.setState(OSearchResult.STATE.FILTER);
          searchResult.setIncluded(ids);
          updateStatistic(index);
          found = true;
          break;
        }
      }
      
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
    final OExpression filter = children.get(1);

      if (filter instanceof OLiteral) {
          final Object iRight = filter.evaluate(context, candidate);
          if (iLeft instanceof Iterable<?>) {
              final Iterable<Object> iterable = (Iterable<Object>) iLeft;

              // CHECK AGAINST A SINGLE VALUE
              for (final Object o : iterable) {
                  if (OEquals.equals(o,iRight)) {
                      return true;
                  }
              }
          } else {
              return OEquals.equals(iLeft,iRight);
          }
      }else{
        //it's not a real contain, it's a filter test
        if (iLeft instanceof Iterable<?>) {
          final Iterable<Object> iterable = (Iterable<Object>) iLeft;

          // CHECK AGAINST A SINGLE VALUE
          for (final Object o : iterable) {
            if(Boolean.TRUE.equals(filter.evaluate(context, o))){
                return true;
            }
          }
        }else{
            return Boolean.TRUE.equals(filter.evaluate(context, iLeft));
        }
      }
    return false;
  }

  @Override
  public OSQLMethodContains copy() {
    final OSQLMethodContains cp = new OSQLMethodContains();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }

}
