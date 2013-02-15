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
package com.orientechnologies.orient.core.sql.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OQuerySource;
import com.orientechnologies.orient.core.sql.model.OSortBy;
import com.orientechnologies.orient.core.sql.parser.CopyVisitor;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import com.orientechnologies.orient.core.sql.parser.UnknownResolverVisitor;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * Executes the SQL SELECT statement. the parse() method compiles the query and 
 * builds the meta information needed by the execute(). If the query contains 
 * the ORDER BY clause, the results are temporary collected internally, 
 * then ordered and finally returned all together to the listener.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class OCommandSelect extends OCommandAbstract implements Iterable {
  
  public static final String KEYWORD_SELECT = "SELECT";

  private final List<OExpression> projections = new ArrayList<OExpression>();
  private OQuerySource source;
  private OExpression filter;
  private final List<OExpression> groupBys = new ArrayList<OExpression>();
  private final List<OSortBy> sortBys = new ArrayList<OSortBy>();
  private long skip;
  
  //result list
  private final List<ODocument> result = new ArrayList<ODocument>();
  
  @Override
  public boolean isIdempotent() {
    return true;
  }
  
  @Override
  public <RET extends OCommandExecutor> RET parse(OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
        
    final OSQLParser.CommandSelectContext candidate = getCommand(iRequest, OSQLParser.CommandSelectContext.class);
    parse(candidate);
      
    if (iRequest instanceof OSQLAsynchQuery) {
      final OSQLAsynchQuery request = (OSQLAsynchQuery)iRequest;
      final OCommandResultListener res = request.getResultListener();
      addListener(new ResultListenerWrap(res));
    }
    
    return (RET)this;
  }

  public <RET extends OCommandExecutor> RET parse(OSQLParser.CommandSelectContext candidate) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
    
    //variables
    projections.clear();
    setLimit(-1);
    skip = -1;
    sortBys.clear();
    groupBys.clear();
    
    //parse projections
    for(OSQLParser.ProjectionContext proj : candidate.projection()){
      if(proj.MULT() != null){
        //all values requested
        projections.clear();
        break;
      }
      projections.add(SQLGrammarUtils.visit(proj));
    }
    
    //parse source
    final OSQLParser.FromContext from = candidate.from();
    source = new OQuerySource();
    source.parse(from);
    
    //parse filter
    if(candidate.filter()!= null){
      filter = SQLGrammarUtils.visit(candidate.filter());
    }else{
      filter = OExpression.INCLUDE;
    }
    
    //parse group by
    if(candidate.groupBy() != null){
      for(OSQLParser.ExpressionContext ele : candidate.groupBy().expression()){
        groupBys.add(SQLGrammarUtils.visit(ele));
      }
    }
    
    //parse sortBy
    if(candidate.orderBy() != null){
      for(OSQLParser.OrderByElementContext ele : candidate.orderBy().orderByElement()){
        final OSortBy.Direction dir = (ele.DESC() == null)? OSortBy.Direction.ASC : OSortBy.Direction.DESC;
        sortBys.add(new OSortBy(SQLGrammarUtils.visit(ele.expression()), dir));
      }
    }
        
    //parse skip
    if(candidate.skip() != null){
      skip = Integer.valueOf(candidate.skip().INT().getText());
    }
    
    //parse limit
    if(candidate.limit() != null){
      setLimit(Integer.valueOf(candidate.limit().INT().getText()));
    }
    
    
    return (RET)this;
  }
  
  public <RET extends OCommandExecutor> RET parse(OSQLParser.SourceContext src, OSQLParser.FilterContext filter) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
    
    //variables
    projections.clear();
    setLimit(-1);
    skip = -1;
    sortBys.clear();
    groupBys.clear();
    
    //parse source
    source = new OQuerySource();
    source.parse(src);
    
    //parse filter
    if(filter != null){
      this.filter = SQLGrammarUtils.visit(filter);
    }else{
      this.filter = OExpression.INCLUDE;
    }
    return (RET)this;
  }
  
  public <RET extends OCommandExecutor> RET parse(String clazz, OSQLParser.FilterContext filter) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
    
    //variables
    projections.clear();
    setLimit(-1);
    skip = -1;
    sortBys.clear();
    groupBys.clear();
    
    //parse source
    source = new OQuerySource();
    source.setTargetClasse(clazz);
    
    //parse filter
    if(filter != null){
      this.filter = SQLGrammarUtils.visit(filter);
    }else{
      this.filter = OExpression.INCLUDE;
    }
    return (RET)this;
  }
  
  
  @Override
  public Collection execute(final Map<Object, Object> iArgs) {
    if(iArgs != null && !iArgs.isEmpty()){
      //we need to set value where we have OUnknowned
      final UnknownResolverVisitor visitor = new UnknownResolverVisitor(iArgs);
      for(int i=0;i<projections.size();i++){
        projections.set(i, (OExpression)projections.get(i).accept(visitor,null));
      }
      if(filter != null){
        filter = (OExpression) filter.accept(visitor, null);
      }
    }
    
    result.clear();
    
    if(groupBys.isEmpty() && sortBys.isEmpty()){
      //normal query
      search(projections, skip, limit, true);
    }else{
      //groupby or sort by search, we must collect all results first
      search(null, -1, -1, false);
      applyGroups();
      applySort();
      
      //notify listeners
      for(OIdentifiable r : result){
        fireResult(r);
      }
    }
    
    return result;
  }
  
  private void search(final List<OExpression> projections, final long skip, final long limit, boolean notifyListeners){
    
    final OCommandContext context = getContext();
    final Iterable<? extends OIdentifiable> target = source.createIterator();
    final Iterator<? extends OIdentifiable> ite = target.iterator();
    
    long nbvalid = 0;
    long nbtested = 0;
    
    while (ite.hasNext()) {
      final ORecord candidate = ite.next().getRecord();

      //filter
      final Object valid = filter.evaluate(context, candidate);
      if (!Boolean.TRUE.equals(valid)) {
        continue;
      }
      nbtested++;

      //check skip
      if (nbtested <= skip) {
        continue;
      }

      nbvalid++;

      //projections
      final ODocument record;
      if (projections != null && !projections.isEmpty()) {
        record = new ODocument();
        record.setIdentity(-1, new OClusterPositionLong(nbvalid - 1));
        evaluate(context, candidate, projections, record);
      } else {
        record = (ODocument) candidate;
      }

      result.add(record);

      //notify listener
      if(notifyListeners){
        if (!fireResult(record)){
          //stop search requested
          break;
        }
      }

      //check limit
      if (limit >= 0 && nbvalid == limit) {
        //reached the limit
        break;
      }
    }
    
  }
  
  /**
   * Build groups for current result list.
   */
  private void applyGroups() {
    if(groupBys.isEmpty()) {
      return;
    }
    
    //build groups
    final Map<Tuple, List<OIdentifiable>> groups = new HashMap<Tuple, List<OIdentifiable>>();
    final Object[] tupleValues = new Object[groupBys.size()];
    for (OIdentifiable rec : result) {
      for (int i = 0; i < tupleValues.length; i++) {
        tupleValues[i] = groupBys.get(i).evaluate(context, rec);
      }
      final Tuple groupTuple = new Tuple((Object[]) tupleValues.clone());
      List<OIdentifiable> group = groups.get(groupTuple);
      if (group == null) {
        group = new ArrayList<OIdentifiable>();
        groups.put(groupTuple, group);
      }
      group.add(rec);
    }
    //we don't need the original result list anymore, we refill it after projections.
    result.clear();

    final CopyVisitor cloner = new CopyVisitor();

    //apply projections
    long inc = 0;
    for (Entry<Tuple, List<OIdentifiable>> group : groups.entrySet()) {
      final List<OIdentifiable> groupEntries = group.getValue();
      //copy projections, they may contain group expressions (svg,min,max,avg,count,...)
      final List<OExpression> groupProjections = new ArrayList<OExpression>();
      for (OExpression ex : projections) {
        groupProjections.add((OExpression) ex.accept(cloner, null));
      }
      //generate a single document for the group
      final ODocument groupDoc = new ODocument();
      groupDoc.setIdentity(-1, new OClusterPositionLong(inc++));
      for (OIdentifiable candidate : groupEntries) {
        evaluate(context, candidate, groupProjections, groupDoc);
      }
      result.add(groupDoc);
    }
  }
  
  /**
   * Sort current result list.
   */
  private void applySort(){
    if(sortBys.isEmpty()){
        return;
      }
    
    final Comparator comparator = OSortBy.createComparator(context, sortBys);
    Collections.sort(result, comparator);
  }
  
  /**
   * Evaluate each expression and store the results in the document.
   * @param projections Projections to evaluate
   * @param doc ODocument to store values
   */
  private static void evaluate(final OCommandContext context, final Object candidate,
          final List<OExpression> projections, final ODocument doc) {
    for (int i = 0, n = projections.size(); i < n; i++) {
      final OExpression projection = projections.get(i);
      String projname = projection.getAlias();
      if (projname == null) {
        projname = String.valueOf(i);
      }
      final Object value = projection.evaluate(context, candidate);
      doc.field(projname, value);
    }
  }
  
  
  // GRAMMAR PARSING ///////////////////////////////////////////////////////////
  

  @Override
  public Iterator iterator() {
    return execute(null).iterator();
  }
  
  private static class Tuple{
    
    private final Object[] values;

    public Tuple(Object[] values) {
      this.values = values;
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 23 * hash + Arrays.deepHashCode(this.values);
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final Tuple other = (Tuple) obj;
      if (!Arrays.deepEquals(this.values, other.values)) {
        return false;
      }
      return true;
    }
    
  }
  
}
