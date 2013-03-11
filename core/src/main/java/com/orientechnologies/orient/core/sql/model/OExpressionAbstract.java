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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.profiler.OJVMProfiler;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public abstract class OExpressionAbstract implements OExpression{

  protected String alias;
  //prepared searched
  protected OSearchResult searchResult;

  public OExpressionAbstract() {
    this(null);
  }

  public OExpressionAbstract(String alias) {
    this.alias = alias;
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public final Object evaluate(OCommandContext context, Object candidate) {    
    if(searchResult != null && candidate instanceof OIdentifiable && searchResult.getState() != OSearchResult.STATE.EVALUATE){
      final Collection<OIdentifiable> included = searchResult.getIncluded();
      final Collection<OIdentifiable> candidates = searchResult.getIncluded();
      final Collection<OIdentifiable> excluded = searchResult.getIncluded();

      if(included == OSearchResult.ALL){
        //guarantee all result match, we can safely ignore the evaluation
        return Boolean.TRUE;
      }else if(excluded == OSearchResult.ALL){
        //guarantee no result match, we can complete skip the search
        return Boolean.FALSE;
      }else{
        //try to avoid evaluation
        if(included != null && included.contains(candidate)){ // included
          return Boolean.TRUE;
        }
        if(candidates != null){ // possible
          if(candidates.contains(candidate)){
            return evaluateNow(context, candidate);
          }else{
            return Boolean.FALSE;
          }
        }
        if(excluded != null){ // definitly not valid
          if(excluded.contains(candidate)){
            return Boolean.FALSE;
          }
        }
      }
    }
    
    return evaluateNow(context, candidate);    
  }
  
  /**
   * Evaluate expression, do not check the OSearchResult.
   * @param context
   * @param candidate
   * @return Object
   */
  protected abstract Object evaluateNow(OCommandContext context, Object candidate);
  
  /**
   * By Default return an SearchResult in STATE.EVALUATE;
   * @param searchContext
   * @return SearchResult, can not be null
   */
  @Override
  public OSearchResult searchIndex(OSearchContext searchContext) {
    this.searchResult = new OSearchResult(this);
    return this.searchResult;
  }
  
  @Override
  public OSearchResult getSearchResult() {
    return searchResult;
  }
  
  @Override
  public final boolean isStatic() {
    return isContextFree() && isDocumentFree();
  }

  /**
   * {@inheritDoc}
   * @return false by default
   */
  @Override
  public boolean isAgregation() {
    return false;
  }

  /**
   * Subclass should override this method when needed.
   * @return false
   */
  @Override
  public boolean isContextFree() {
    return false;
  }

  /**
   * Subclass should override this method when needed.
   * @return false
   */
  @Override
  public boolean isDocumentFree() {
    return false;
  }

  @Override
  public final String toString() {
    return toString(this);
  }

  protected String thisToString() {
    return this.getClass().getSimpleName() + ((alias!=null)? " AS "+alias : "");
  }

  private static String toString(OExpression candidate){
    if(candidate instanceof OExpressionAbstract){
      final OExpressionAbstract exp = (OExpressionAbstract) candidate;
      final StringBuilder sb = new StringBuilder();
      sb.append(exp.thisToString());
      if(candidate.getAlias() != null){
        sb.append("  AS ").append(candidate.getAlias());
      }
      
      if(candidate instanceof OExpressionWithChildren){
        final OExpressionWithChildren fct = (OExpressionWithChildren) candidate;
        final List<OExpression> children = fct.getChildren();
        //print childrens
        final int nbChild = children.size();
        if(nbChild>0){
            sb.append('\n');
            for(int i=0;i<nbChild;i++){
                if(i==nbChild-1){
                    sb.append("\u2514\u2500 ");
                }else{
                    sb.append("\u251C\u2500 ");
                }

                String sc = toString(children.get(i));
                String[] parts = sc.split("\n");
                sb.append(parts[0]).append('\n');
                for(int k=1;k<parts.length;k++){
                    if(i==nbChild-1){
                        sb.append(' ');
                    }else{
                        sb.append('\u2502');
                    }
                    sb.append("    ");
                    sb.append(parts[k]);
                    sb.append('\n');
                }
            }
        }
      }
      return sb.toString();
    }else{
      return candidate.toString();
    }
  }
  
  protected ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  /**
   * Register statistic information about usage of index in {@link OProfiler}.
   * 
   * @param index
   *          which usage is registering.
   */
  public void updateStatistic(OIndex<?> index) {

    final OJVMProfiler profiler = Orient.instance().getProfiler();
    if (profiler.isRecording()) {
      Orient.instance().getProfiler()
          .updateCounter(profiler.getDatabaseMetric(index.getDatabaseName(), "query.indexUsed"), "Used index in query", +1);

      final int paramCount = index.getDefinition().getParamCount();
      if (paramCount > 1) {
        final String profiler_prefix = profiler.getDatabaseMetric(index.getDatabaseName(), "query.compositeIndexUsed");
        profiler.updateCounter(profiler_prefix, "Used composite index in query", +1);
        profiler.updateCounter(profiler_prefix + "." + paramCount, "Used composite index in query with " + paramCount + " params",
            +1);
      }
    }
  }
  
  @Override
  public OExpression copy() {
    try {
      return (OExpression)this.clone();
    } catch (CloneNotSupportedException e) {
      throw new OException(e.getMessage(),e);
    }
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this,data);
  }
}
