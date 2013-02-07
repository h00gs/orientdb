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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.parser.OSQL;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import com.orientechnologies.orient.core.sql.parser.SyntaxException;
import com.orientechnologies.orient.core.sql.parser.UnknownResolverVisitor;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * Executes the SQL SELECT statement. the parse() method compiles the query and 
 * builds the meta information needed by the execute(). If the query contains 
 * the ORDER BY clause, the results are temporary collected internally, 
 * then ordered and finally returned all together to the listener.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class OCommandSelect extends OCommandAbstract{
  
  public static final String KEYWORD_SELECT = "SELECT";

  private List<OExpression> projections;
  private String source;
  private OExpression filter;
  
  //keep the base request to notify result changes.
  private OSQLAsynchQuery<ORecordSchemaAware<?>> request;
  
  @Override
  public boolean isIdempotent() {
    return true;
  }
  
  @Override
  public <RET extends OCommandExecutor> RET parse(OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
    
    final String sql = ((OCommandRequestText) iRequest).getText();
    System.err.println("|||||||||||||||||||| "+ sql);
    final ParseTree tree = OSQL.compileExpression(sql);
    if(tree instanceof OSQLParser.CommandContext){
      try {
        visit((OSQLParser.CommandContext)tree);
      } catch (SyntaxException ex) {
        throw new OException(ex.getMessage(), ex);
      }
    }else{
      throw new OException("Parse error, query is not a valid INSERT INTO.");
    }
        
    if (iRequest instanceof OSQLAsynchQuery) {
      request = (OSQLAsynchQuery)iRequest;
    }
    
    return (RET)this;
  }

  @Override
  public Object execute(final Map<Object, Object> iArgs) {
    if(iArgs != null && !iArgs.isEmpty()){
      //we need to set value where we have OUnknowned
      final UnknownResolverVisitor visitor = new UnknownResolverVisitor(iArgs);
      for(int i=0;i<projections.size();i++){
        projections.set(i, (OExpression)projections.get(i).accept(visitor,null));
      }
    }
    
    final ODatabaseRecord db = getDatabase();
    final OSchema schema = db.getMetadata().getSchema();
    final OClass clazz = schema.getClass(source);
    final int[] clusters = clazz.getClusterIds();
    final OCommandContext context = getContext();
    
    final List result = new ArrayList();
    
//    final OIdentifiableIterator<ORecordInternal<?>> target = new ORecordIteratorClass<ORecordInternal<?>>(
//            db, (ODatabaseRecordAbstract) db, source, true).setRange(null, null);
//    
//    long nbvalid = 0;
//    clustersSearch:
//    while(target.hasNext()){
//      final ORecord rec = target.next().getRecord();
//      //filter
//      final Object valid = filter.evaluate(context, rec);
//      if(!Boolean.TRUE.equals(valid)){
//        continue;
//      }
//
//      //check limit
//      nbvalid++;
//      result.add(rec);
//      if(limit>=0 && nbvalid==limit){
//        //reached the limit
//        break clustersSearch;
//      }
//    }
    
    //iterate on all clusters
    long nbvalid = 0;
    clustersSearch:
    for(int clusterId : clusters){
      final String clusterName = db.getClusterNameById(clusterId);
      final ORecordIteratorCluster ite = db.browseCluster(clusterName);
      
      //iterate on all datas
      while(ite.hasNext()){
        final ORecord rec = ite.next().getRecord();
        
        //filter
        final Object valid = filter.evaluate(context, rec);
        if(!Boolean.TRUE.equals(valid)){
          continue;
        }
        
        result.add(rec);
        
        //notify listener if needed
        if(request != null){
          final OCommandResultListener listener = request.getResultListener();
          if(listener != null){
            if(!listener.result(rec)){
              //stop search requested
              break clustersSearch;
            }
          }
        }
        
        //check limit
        nbvalid++;
        if(limit>=0 && nbvalid==limit){
          //reached the limit
          break clustersSearch;
        }
      }
    }
    
    return result;
  }
  
  // GRAMMAR PARSING ///////////////////////////////////////////////////////////
  
  private void visit(OSQLParser.CommandContext candidate) throws SyntaxException {
    final Object commandTree = candidate.getChild(0);
    if(commandTree instanceof OSQLParser.CommandSelectContext){
      visit((OSQLParser.CommandSelectContext)commandTree);
    }else{
      throw new OException("Unknowned command " + candidate.getClass()+" "+candidate);
    }
  }
  
  private void visit(OSQLParser.CommandSelectContext candidate) throws SyntaxException{    
    //variables
    projections = new ArrayList<OExpression>();
    setLimit(-1);
    
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
    source = from.word().getText();
    
    //parse filter
    if(candidate.filter()!= null){
      filter = SQLGrammarUtils.visit(candidate.filter());
    }else{
      filter = OExpression.INCLUDE;
    }
    
    if(candidate.limit() != null){
      setLimit(Integer.valueOf(candidate.limit().INT().getText()));
    }
    
  }
  
  
}
