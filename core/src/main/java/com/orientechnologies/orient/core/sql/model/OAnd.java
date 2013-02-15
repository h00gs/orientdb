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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OAnd extends OExpressionWithChildren{

  public OAnd(OExpression left, OExpression right) {
    this(null,left,right);
  }

  public OAnd(String alias, OExpression left, OExpression right) {
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
    return "(And)";
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    final Object objLeft = children.get(0).evaluate(context, candidate);
    if(!(objLeft instanceof Boolean)){
      //can not combine non boolean values
      return false;
    }
    final Object objRight = children.get(1).evaluate(context, candidate);
    if(!(objRight instanceof Boolean)){
      //can not combine non boolean values
      return false;
    }
    
    return ((Boolean)objLeft) && ((Boolean)objRight);
  }

  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {
    //combine search results for left and right filters.
    final OSearchResult resLeft = getLeft().getSearchResult();
    final OSearchResult resRight = getRight().getSearchResult();
    
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
    return new OAnd(alias, getLeft(), getRight());
  }
  
}
