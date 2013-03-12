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

import com.orientechnologies.common.collection.OAlwaysGreaterKey;
import com.orientechnologies.common.collection.OAlwaysLessKey;
import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OAnd extends OExpressionWithChildren{

  public OAnd(List<OExpression> arguments) {
    super(arguments);
  }

  public OAnd(String alias, List<OExpression> arguments) {
    super(alias, arguments);
  }

  public OAnd(String alias, OExpression... children) {
    super(alias, children);
  }
  
  public OAnd(OExpression left, OExpression right) {
    this(null,left,right);
  }

  @Override
  protected String thisToString() {
    return "(And)";
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    for(OExpression e : children){
      final Object val = e.evaluate(context, candidate);
      if(!(val instanceof Boolean)){
        //can not combine non boolean values
        return Boolean.FALSE;
      }
      if(Boolean.FALSE.equals(val)){
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }
 
  public OSearchResult searchIndex(OSearchContext searchContext) {
    searchResult = new OSearchResult(this);
        
    final String className = searchContext.getSource().getTargetClasse();
    combineIndex:
    if (className != null) {
      //see if we have a multikey index for pattern : field1 <op> value1 AND field2 <op> value2 ...
      final Map<String,FieldRange> ranges = new HashMap<String,FieldRange>();      
      for(OExpression exp : children){
        final FieldRange range = toFieldRange(exp);
        if(range == null){
          //no optimization
          break combineIndex;
        }
        
        if(ranges.containsKey(range.fieldName)){
          //merge ranges
          final Object mergeResult = ranges.get(range.fieldName).merge(range);
          if(mergeResult == null){
            //conditions makes it so nothing can match
            searchResult.setState(OSearchResult.STATE.FILTER);
            searchResult.setExcluded(OSearchResult.ALL);
            return searchResult;
          }else if(Boolean.FALSE.equals(mergeResult)){
            //merge failed, index not usable
            break combineIndex;
          }else{
            //merge succesfull
          }
        }else{
          ranges.put(range.fieldName,range);
        }
      }
      
      //search for an index
      final OClass clazz = getDatabase().getMetadata().getSchema().getClass(className);
      final Set<OIndex<?>> indexes = clazz.getClassInvolvedIndexes(ranges.keySet());
      if (indexes == null || indexes.isEmpty()) {
        //no index usable
        break combineIndex;
      }
      
      for (OIndex index : indexes) {
        //found a usable index
        final List<String> fieldKeys = index.getDefinition().getFields();
        final Object[] fkmin = new Object[fieldKeys.size()];
        final Object[] fkmax = new Object[fieldKeys.size()];
        boolean minInc = true;
        boolean maxInc = true;
        for(int i=0;i<fkmin.length;i++){
          final String name = fieldKeys.get(i);
          final FieldRange range = ranges.get(name);
          if(range == null){
            fkmin[i] = new OAlwaysLessKey();
            fkmax[i] = new OAlwaysGreaterKey();
            minInc = true;
            maxInc = true;
          }else{
            fkmin[i] = range.min;
            fkmax[i] = range.max;
            minInc = range.isMinInclusive;
            maxInc = range.isMaxInclusive;
          }
        }
        
        final OCompositeKey minkey = new OCompositeKey(fkmin);
        final OCompositeKey maxkey = new OCompositeKey(fkmax);
        final Collection<OIdentifiable> ids = index.getValuesBetween(minkey, minInc, maxkey, maxInc);
        searchResult.setState(OSearchResult.STATE.FILTER);
        searchResult.setIncluded(ids);
        updateStatistic(index);
        return searchResult;
      }      
    }
    
    //no composite key index could be used, use basic merge.    
    for(OExpression child : children){
      //children search optimization
      child.searchIndex(searchContext);
    }
    analyzeSearchIndex(searchContext, searchResult);
    return searchResult;
  }
  
  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {

    //no combined key index could be used, fallback on left/right merge.
    final OSearchResult resLeft = new OSearchResult(this);
    result.set(children.get(0).getSearchResult());
    for (int i = 1, n = children.size(); i < n; i++) {
      resLeft.set(result);
      result.reset();
      combineSearch(resLeft, children.get(i), result);
    }
  }

  private void combineSearch(OSearchResult resLeft, OExpression right, OSearchResult result){
    //combine search results for left and right filters.
    final OSearchResult resRight = right.getSearchResult();
    
    if(resLeft.getState() == OSearchResult.STATE.EVALUATE || resRight.getState() == OSearchResult.STATE.EVALUATE){
      //we can't reduce global search, all elements will have to be tested
      return;
    }
    
    result.setState(OSearchResult.STATE.FILTER);
      
    final Collection<OIdentifiable> leftIncluded = resLeft.getIncluded();
    final Collection<OIdentifiable> leftCandidates = resLeft.getCandidates();
    final Collection<OIdentifiable> leftExcluded = resLeft.getExcluded();
    final Collection<OIdentifiable> rightIncluded = resRight.getIncluded();
    final Collection<OIdentifiable> rightCandidates = resRight.getCandidates();
    final Collection<OIdentifiable> rightExcluded = resRight.getExcluded();
    
    
    if(leftExcluded == OSearchResult.ALL || rightExcluded == OSearchResult.ALL){
      //no result will ever match
      result.setExcluded(OSearchResult.ALL);
      return;
    }
    
    if(leftIncluded == OSearchResult.ALL){
      //we can copy the right condition result to reduce search
      result.setIncluded(rightIncluded);
      result.setCandidates(rightCandidates);
      result.setExcluded(rightExcluded);
      return;
    }else if(rightIncluded == OSearchResult.ALL){
      //we can copy the left condition result to reduce search
      result.setIncluded(leftIncluded);
      result.setCandidates(leftCandidates);
      result.setExcluded(leftExcluded);
      return;
    }
    
    
    //combine results
    final Set<OIdentifiable> included;
    final Set<OIdentifiable> candidates;
    final Set<OIdentifiable> excluded;
    //knowing a OSearchResult can only have Included+Candidates or Excluded
    if(leftIncluded != null || leftCandidates != null){
      included = new HashSet<OIdentifiable>();
      candidates = new HashSet<OIdentifiable>();
      excluded = null;
      
      if(rightIncluded != null || rightCandidates != null){        
        //cross lists of all possible records
        if(leftIncluded != null) candidates.addAll(leftIncluded);
        if(leftCandidates != null) candidates.addAll(leftCandidates);
        if(rightIncluded != null) candidates.retainAll(rightIncluded);
        if(rightCandidates != null) candidates.retainAll(rightCandidates);
        
        included.addAll(candidates);
        //keep in included only those in both included list
        if(leftIncluded != null) candidates.retainAll(leftIncluded);
        if(rightIncluded != null) candidates.retainAll(rightIncluded);
        //remove included for candidates list
        candidates.removeAll(included);
        
      }else{
        //right has exclusion
        if(leftIncluded != null) included.addAll(leftIncluded);
        if(leftCandidates != null) candidates.addAll(leftCandidates);
        included.removeAll(rightExcluded);
        candidates.removeAll(rightExcluded);
      }
    }else{
      if(rightIncluded != null || rightCandidates != null){  
        //right has candidates
        included = new HashSet<OIdentifiable>();
        candidates = new HashSet<OIdentifiable>();
        excluded = null;
        if(rightIncluded != null) included.addAll(rightIncluded);
        if(rightCandidates != null) candidates.addAll(rightCandidates);
        included.removeAll(leftExcluded);
        candidates.removeAll(leftExcluded);
        
      }else{
        //right has exclusion
        //combine exclusion list
        included = null;
        candidates = null;
        excluded = new HashSet<OIdentifiable>(leftExcluded);
        excluded.addAll(rightExcluded);
      }
    }
    
    result.setIncluded(included);
    result.setCandidates(candidates);
    result.setExcluded(excluded);
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
  public OAnd copy() {
    return new OAnd(alias,new ArrayList<OExpression>(getChildren()));
  }
  
  private static FieldRange toFieldRange(OExpression candidate){
    if(candidate instanceof ORangedFilter && !(candidate instanceof ONotEquals) ){
      //test is equality match pattern : field op value
      final ORangedFilter exp = (ORangedFilter) candidate;
      OName fieldName;
      Object value;
      final boolean flip;
      if(exp.getLeft() instanceof OName && exp.getRight().isStatic()){
        fieldName = (OName) exp.getLeft();
        value = exp.getRight().evaluate(null, null);
        flip = false;
      }else if(exp.getLeft().isStatic() && exp.getRight() instanceof OName){
        fieldName = (OName) exp.getRight();
        value = exp.getLeft().evaluate(null, null);
        flip = true;
      }else{
        //no optimisation
        return null;
      }
      
      final FieldRange fr = new FieldRange();
      fr.fieldName = fieldName.getName();
      if(candidate instanceof OEquals){
        fr.min = fr.max = value;
        fr.isMinInclusive = fr.isMaxInclusive = true;
      }else if(candidate instanceof OInferior){
        if(flip){
          fr.max = new OAlwaysGreaterKey();
          fr.isMaxInclusive = true;
          fr.min = value;
          fr.isMinInclusive = false;
        }else{
          fr.max = value;
          fr.isMaxInclusive = false;
          fr.min = new OAlwaysLessKey();
          fr.isMinInclusive = true;
        }
      }else if(candidate instanceof OInferiorEquals){
        if(flip){
          fr.max = new OAlwaysGreaterKey();
          fr.isMaxInclusive = true;
          fr.min = value;
          fr.isMinInclusive = true;
        }else{
          fr.max = value;
          fr.isMaxInclusive = true;
          fr.min = new OAlwaysLessKey();
          fr.isMinInclusive = true;
        }
      }else if(candidate instanceof OSuperior){
        if(flip){
          fr.max = value;
          fr.isMaxInclusive = false;
          fr.min = new OAlwaysLessKey();
          fr.isMinInclusive = true;
        }else{
          fr.max = new OAlwaysGreaterKey();
          fr.isMaxInclusive = true;
          fr.min = value;
          fr.isMinInclusive = false;
        }
      }else if(candidate instanceof OSuperiorEquals){
        if(flip){
          fr.max = value;
          fr.isMaxInclusive = true;
          fr.min = new OAlwaysLessKey();
          fr.isMinInclusive = true;
        }else{
          fr.max = new OAlwaysGreaterKey();
          fr.isMaxInclusive = true;
          fr.min = value;
          fr.isMinInclusive = true;
        }
      }
      
      return fr;
    }else if(candidate instanceof OBetween){
      final OBetween between = (OBetween) candidate;
      if(between.getTarget() instanceof OName && between.getLeft().isStatic() && between.getRight().isStatic()){
        final FieldRange fr = new FieldRange();
        fr.fieldName = ((OName)between.getTarget()).getName();
        fr.min = between.getLeft().evaluate(null, null);
        fr.max = between.getRight().evaluate(null, null);
        fr.isMinInclusive = true;
        fr.isMaxInclusive = true;
        return fr;
      }
    }
    
    return null;
  }
  
  private static class FieldRange{
    String fieldName;
    Object min;
    Object max;
    boolean isMinInclusive;
    boolean isMaxInclusive;
    
    public boolean isPonctual(){
      return min == max;
    }
    
    public boolean contains(Object candidate){
      if(min instanceof OAlwaysLessKey){
        //ok
      }else{
        try{
          final int c = ((Comparable)min).compareTo(candidate);
          if(c > 0){
            return false;
          }else if(c == 0 && !isMinInclusive){
            return false;
          }
        }catch(Exception ex){
          //we tryed
          return false;
        }
      }
      
      if(max instanceof OAlwaysGreaterKey){
        //ok
      }else{
        try{
          final int c = ((Comparable)max).compareTo(candidate);
          if(c < 0){
            return false;
          }else if(c == 0 && !isMaxInclusive){
            return false;
          }
        }catch(Exception ex){
          //we tryed
          return false;
        }
      }
      
      return true;
    }
    
    /**
     * Try to merge range
     * @param range
     * @return true if merge is successful.
     *         false if merge is impossible
     *         null if merge result in an impossible match
     */
    private Object merge(FieldRange range){
      if(isPonctual() && range.isPonctual()){
        if(this.min.equals(range.min)){
          //same
          return true;
        }else{
          //ranges are different ponctual values
          //nothing can match
          return null;
        }
      }else if(isPonctual()){
        //one ponctual, one range
        return range.contains(min);
      }else if(range.isPonctual()){
        //one range, one ponctual
        return contains(range.min);
      }
      
      if(min instanceof OAlwaysLessKey){
        min = range.min;
        isMinInclusive = range.isMinInclusive;
      }else if(range.min instanceof OAlwaysLessKey){
        //keep current
      }else{
        //keep the maximum value
        try{
          if(min instanceof Comparable){
            if(((Comparable)min).compareTo(range.min) >=0){
              //keep curent
              isMinInclusive = isMinInclusive && range.isMinInclusive;
            }else{
              min = range.min;
              isMinInclusive = range.isMinInclusive;
            }
          }else{
            return false;
          }
        }catch(Exception ex){
          //we tryed
          return false;
        }
      }
      
      if(max instanceof OAlwaysGreaterKey){
        max = range.max;
        isMaxInclusive = range.isMaxInclusive;
      }else if(range.max instanceof OAlwaysGreaterKey){
        //keep current
      }else{
        //keep the minimum value
        try{
          if(max instanceof Comparable){
            if(((Comparable)max).compareTo(range.max) <=0){
              //keep current
              isMaxInclusive = isMaxInclusive && range.isMaxInclusive;
            }else{
              max = range.max;
              isMaxInclusive = range.isMaxInclusive;
            }
          }else{
            return false;
          }
        }catch(Exception ex){
          //we tryed
          return false;
        }
      }
      
      return true;
    }    
  }
  
}
