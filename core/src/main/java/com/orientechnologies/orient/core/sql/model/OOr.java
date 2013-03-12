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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OOr extends OExpressionWithChildren{

  public OOr(List<OExpression> arguments) {
    super(arguments);
  }

  public OOr(String alias, List<OExpression> arguments) {
    super(alias, arguments);
  }

  public OOr(String alias, OExpression... children) {
    super(alias, children);
  }

  public OOr(OExpression left, OExpression right) {
    this(null,left,right);
  }
  
  @Override
  protected String thisToString() {
    return "(Or)";
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    for(OExpression e : children){
      final Object val = e.evaluate(context, candidate);
      if(Boolean.TRUE.equals(val)){
        return Boolean.TRUE;
      }
    }
    return Boolean.FALSE;
  }

  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {
    
    //no combined key index could be used, fallback on left/right merge.
    final OSearchResult resLeft = new OSearchResult(this);
    result.set(children.get(0).getSearchResult());
    for(int i=1,n=children.size();i<n;i++){
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
    
    if(leftIncluded == OSearchResult.ALL || rightIncluded == OSearchResult.ALL){
      //all result will match
      result.setIncluded(OSearchResult.ALL);
      return;
    }
    
    if(leftExcluded == OSearchResult.ALL){
      //we can copy the right condition result to reduce search
      result.setIncluded(rightIncluded);
      result.setCandidates(rightCandidates);
      result.setExcluded(rightExcluded);
      return;
    }else if(rightExcluded == OSearchResult.ALL){
      //we can copy the left condition result to reduce search
      result.setIncluded(leftIncluded);
      result.setCandidates(leftCandidates);
      result.setExcluded(leftExcluded);
      return;
    }
    
    //merge result
    final Set<OIdentifiable> included;
    final Set<OIdentifiable> candidates;
    final Set<OIdentifiable> excluded;
    //knowing a OSearchResult can only have Included+Candidates or Excluded
    if(leftExcluded != null && rightExcluded != null){
      //cross exclusion list
      excluded = new HashSet<OIdentifiable>(leftExcluded);
      included = null;
      candidates = null;
      excluded.retainAll(rightExcluded);
      return;      
    }else if(leftExcluded != null || rightExcluded != null){
      //we can only preserve exclusion list
      excluded = new HashSet<OIdentifiable>();
      if(leftExcluded != null) excluded.addAll(leftExcluded);
      if(rightExcluded != null) excluded.addAll(rightExcluded);
      included = null;
      candidates = null;
    }else{
      //merge included and candidates
      included = new HashSet<OIdentifiable>();
      candidates = new HashSet<OIdentifiable>();
      excluded = null;
      if(leftIncluded != null) included.addAll(leftIncluded);
      if(rightIncluded != null) included.addAll(rightIncluded);
      if(leftCandidates != null) candidates.addAll(leftCandidates);
      if(rightCandidates != null) candidates.addAll(rightCandidates);
      candidates.removeAll(included);
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
  public OOr copy() {
    return new OOr(alias,new ArrayList<OExpression>(getChildren()));
  }
  
}
