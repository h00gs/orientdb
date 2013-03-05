/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 * Copyright 2013 Geomatys.
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
package com.orientechnologies.orient.core.sql.command;

import java.util.*;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OQuerySource;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import com.orientechnologies.orient.core.sql.parser.OToSQLVisitor;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;

import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * Executes a TRAVERSE crossing records. Returns a List<OIdentifiable> containing all the traversed records that match the WHERE
 * condition.
 * <p>
 * SYNTAX: <code>TRAVERSE <field>* FROM <target> WHERE <condition></code>
 * </p>
 * <p>
 * In the command context you've access to the variable $depth containing the depth level from the root node. This is useful to
 * limit the traverse up to a level. For example to consider from the first depth level (0 is root node) to the third use:
 * <code>TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3</code>. To filter traversed records use it combined with a SELECT
 * statement:
 * </p>
 * <p>
 * <code>SELECT FROM (TRAVERSE children FROM #5:23 WHERE $depth BETWEEN 1 AND 3) WHERE city.name = 'Rome'</code>
 * </p>
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 */
public class OCommandTraverse extends OCommandAbstract implements Iterable {

  public static final String KEYWORD_TRAVERSE = "TRAVERSE";
  private static final OToSQLVisitor TOSQL = new OToSQLVisitor(false, false);

  private final List<String> projections = new ArrayList<String>();
  private OQuerySource source;
  private OExpression filter;

  public OCommandTraverse() {
  }

  public OCommandTraverse parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandTraverseContext candidate = getCommand(iRequest, OSQLParser.CommandTraverseContext.class);

    //check if we have listeners
    if (iRequest instanceof OSQLAsynchQuery) {
      final OSQLAsynchQuery request = (OSQLAsynchQuery)iRequest;
      final OCommandResultListener res = request.getResultListener();
      addListener(new ResultListenerWrap(res));
    }

    parse(candidate);

    return this;
  }

  public OCommandTraverse parse(final OSQLParser.CommandTraverseContext candidate) {
        final ODatabaseRecord database = getDatabase();
        database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

        //parse projections
        for(OSQLParser.TraverseProjectionContext proj : candidate.traverseProjection()){
            final String p;
            if(proj.MULT() != null){
                p = "*";
            }else if(proj.traverseAll() != null){
                p = "any()";
            }else if(proj.traverseAny() != null){
                p = "all()";
            }else{
                final OExpression exp = SQLGrammarUtils.visit(proj.expression());
                p = String.valueOf(exp.accept(TOSQL,null));
            }
            projections.add(p);
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

        //parse limit
        if(candidate.limit() != null){
            setLimit(Integer.valueOf(candidate.limit().INT().getText()));
        }

        return this;
    }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  @Override
  public Collection execute(Map<Object, Object> iArgs) {
    final OTraverse trs = new OTraverse();
    trs.predicate(filter);
    trs.target(source.createIterator());
    trs.fields(projections);
    trs.limit(getLimit());
    final List<OIdentifiable> result = trs.execute();

    //notify listeners
    for(OIdentifiable r : result){
      fireResult(r);
    }

    return result;
  }

  @Override
  public Iterator iterator() {
    return execute(null).iterator();
  }

  public String getSyntax() {
    return "TRAVERSE <field>* FROM <target> [WHILE <condition>] [LIMIT int]";
  }
  
}
