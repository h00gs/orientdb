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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public final class OSearchResult {
    
  /**
   * Key used when storing searchcontext in OCommandContext.
   */
  public static final String CONTEXT_KEY = "searchresult";
  
  public static enum STATE{
    /** indicate this expression does not always result in a boolean, 
     * result is unknown */
    EVALUATE,
    /** indicate this expression result in a boolean, 
     * using the included/candidates/excluded collection is safe */
    FILTER
  }
  
  /**
   * Iterable used to explain it matches ALL results.
   */
  public static final Collection ALL = Collections.unmodifiableSet(new HashSet());
  
  private final OExpression filter;
  private STATE state = STATE.EVALUATE;
  
  private Collection<OIdentifiable> included = null;  
  private Collection<OIdentifiable> candidates = null;  
  private Collection<OIdentifiable> excluded = null;

  public OSearchResult(final OExpression filter) {
    this.filter = filter;
  }

  public STATE getState() {
    return state;
  }

  public void setState(STATE state) {
    this.state = state;
  }

  /**
   * Expression which returned this search result.
   */
  public OExpression getFilter() {
    return filter;
  }
  
  /**
   * Records guaranted to be match the filter.
   * Filter evaluation is guaranted to be true.
   */
  public Collection<OIdentifiable> getIncluded() {
    return included;
  }

  public void setIncluded(Collection<OIdentifiable> included) {
    this.included = included;
  }

  /**
   * Records likely to match filter.
   * Records not in the include list and not in this list are guarantee to be excluded.
   * Filter evaluation is needed ensure the candidate matches the filter.
   */
  public Collection<OIdentifiable> getCandidates() {
    return candidates;
  }

  public void setCandidates(Collection<OIdentifiable> candidates) {
    this.candidates = candidates;
  }

  /**
   * Records guaranted to not match the filter.
   * Filter evaluation is guaranted to be false.
   */
  public Collection<OIdentifiable> getExcluded() {
    return excluded;
  }

  public void setExcluded(Collection<OIdentifiable> excluded) {
    this.excluded = excluded;
  }
  
}
