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

import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandListener;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * SQL DELETE VERTEX command.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 */
public class OCommandDeleteVertex extends OCommandAbstract implements OCommandListener {
  
  public static final String NAME = "DELETE VERTEX";
  
  private OSQLParser.SourceContext source;
  private OSQLParser.FilterContext filter;
  private long recordCount = 0;

  public OCommandDeleteVertex() {
  }

  public OCommandDeleteVertex parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandDeleteVertexContext candidate = getCommand(iRequest, OSQLParser.CommandDeleteVertexContext.class);
    
    source = candidate.source();
    filter = candidate.filter();
        
    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    recordCount = 0;
    final OCommandSelect subselect = new OCommandSelect();    
    if(source == null){
      //delete all vertices
      subselect.parse("V", filter);
    }else{
      subselect.parse(source, filter);
    }
    subselect.addListener(this);
    subselect.execute(iArgs);
    return recordCount;
  }

  @Override
  public String getSyntax() {
    return "DELETE VERTEX <rid>|<[<class>] [WHERE <conditions>] [LIMIT <max-records>]>";
  }
  
  
  //////////////////////////////////////////////////////////////////////////////
  //Sub select events //////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  
  @Override
  public boolean result(Object iRecord) {
    final ORecordAbstract<?> record = (ORecordAbstract<?>) iRecord;

    final ORID id = record.getIdentity();
    if (id.isValid()) {
      if (((OGraphDatabase) getDatabase()).removeVertex(id)) {
        recordCount++;
      }
    }
    return true;
  }

  @Override
  public void end() {
  }

  @Override
  public void onBegin(Object iTask, long iTotal) {
  }

  @Override
  public boolean onProgress(Object iTask, long iCounter, float iPercent) {
    return true;
  }

  @Override
  public void onCompletition(Object iTask, boolean iSucceed) {
  }
  
}
