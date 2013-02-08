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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OQuerySource;
import com.orientechnologies.orient.core.sql.parser.OSQL;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import com.orientechnologies.orient.core.sql.parser.SyntaxException;
import com.orientechnologies.orient.core.sql.parser.UnknownResolverVisitor;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.antlr.v4.runtime.tree.ParseTree;

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

  private List<OExpression> projections;
  private OQuerySource source;
  private OExpression filter;
  private long skip;
  
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
      final Object commandTree = tree.getChild(0);
      if(commandTree instanceof OSQLParser.CommandSelectContext){
        try {
          visit((OSQLParser.CommandSelectContext)commandTree);
        } catch (SyntaxException ex) {
          throw new OException(ex.getMessage(), ex);
        }
      }else{
        throw new OException("Unknowned command " + commandTree.getClass()+" "+commandTree);
      }
    }else{
      throw new OException("Parse error, query is not a valid INSERT INTO.");
    }
        
    if (iRequest instanceof OSQLAsynchQuery) {
      request = (OSQLAsynchQuery)iRequest;
    }
    
    return (RET)this;
  }

  public <RET extends OCommandExecutor> RET parse(OSQLParser.CommandSelectContext ast) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
    try {
      visit(ast);
    } catch (SyntaxException ex) {
      throw new OException("Parse error, query is not a valid INSERT INTO.");
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
    }
    
    final OCommandContext context = getContext();
    final List result = new ArrayList();
    final Iterable<? extends OIdentifiable> target = source.createIterator();
    final Iterator<? extends OIdentifiable> ite = target.iterator();
    
    long nbvalid = 0;
    long nbtested = 0;
    
    clustersSearch:
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
      if (!projections.isEmpty()) {
        record = new ODocument();
        record.setIdentity(-1, new OClusterPositionLong(nbvalid - 1));
        for (int i = 0, n = projections.size(); i < n; i++) {
          final OExpression projection = projections.get(i);
          String projname = projection.getAlias();
          if (projname == null) {
            projname = String.valueOf(i);
          }
          final Object value = projection.evaluate(context, candidate);
          record.field(projname, value);
        }
      } else {
        record = (ODocument) candidate;
      }

      result.add(record);

      //notify listener if needed
      if (request != null) {
        final OCommandResultListener listener = request.getResultListener();
        if (listener != null) {
          if (!listener.result(record)) {
            //stop search requested
            break clustersSearch;
          }
        }
      }

      //check limit
      if (limit >= 0 && nbvalid == limit) {
        //reached the limit
        break clustersSearch;
      }
    }
    
    return result;
  }
  
  // GRAMMAR PARSING ///////////////////////////////////////////////////////////
    
  private void visit(OSQLParser.CommandSelectContext candidate) throws SyntaxException{    
    //variables
    projections = new ArrayList<OExpression>();
    setLimit(-1);
    skip = -1;
    
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
    
    //parse skip
    if(candidate.skip() != null){
      skip = Integer.valueOf(candidate.skip().INT().getText());
    }
    
    //parse limit
    if(candidate.limit() != null){
      setLimit(Integer.valueOf(candidate.limit().INT().getText()));
    }
    
  }

  @Override
  public Iterator iterator() {
    return execute(null).iterator();
  }
  
}
