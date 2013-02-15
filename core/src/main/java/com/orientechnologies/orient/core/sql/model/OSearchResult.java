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
import java.util.Collections;
import java.util.HashSet;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public final class OSearchResult {
  
  /**
   * Iterable used to explain it matches ALL results.
   */
  public static final Iterable ALL = Collections.unmodifiableSet(new HashSet());
  
  private final OExpression filter;
  
  private Iterable<OIdentifiable> included = null;
  
  private Iterable<OIdentifiable> candidates = null;
  
  private Iterable<OIdentifiable> excluded = null;

  public OSearchResult(final OExpression filter) {
    this.filter = filter;
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
  public Iterable<OIdentifiable> getIncluded() {
    return included;
  }

  /**
   * Records likely to match filter.
   * Records not in the include list and not in this list are guarantee to be excluded.
   * Filter evaluation is needed ensure the candidate matches the filter.
   */
  public Iterable<OIdentifiable> getCandidates() {
    return candidates;
  }

  /**
   * Records guaranted to not match the filter.
   * Filter evaluation is guaranted to be false.
   */
  public Iterable<OIdentifiable> getExcluded() {
    return excluded;
  }
  
}
