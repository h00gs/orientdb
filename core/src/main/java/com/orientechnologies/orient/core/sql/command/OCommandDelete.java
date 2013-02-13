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
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;

/**
 * SQL DELETE command.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 */
public class OCommandDelete extends OCommandAbstract implements OCommandListener {
  public static final String NAME = "DELETE FROM";

  private OSQLParser.SourceContext source;
  private OSQLParser.FilterContext filter;
  private long recordCount = 0;
  
  public OCommandDelete() {
  }

  public OCommandDelete parse(final OCommandRequest iRequest) {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandDeleteContext candidate = getCommand(iRequest, OSQLParser.CommandDeleteContext.class);
    source = candidate.from().source();
    filter = candidate.filter();
    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    recordCount = 0;
    final OCommandSelect subselect = new OCommandSelect();
    subselect.parse(source, filter);
    subselect.addListener(this);
    subselect.execute(iArgs);
    return recordCount;
  }

  public String getSyntax() {
    return "DELETE FROM <Class>|RID|cluster:<cluster> [WHERE <condition>*]";
  }
  
  
  //////////////////////////////////////////////////////////////////////////////
  //Sub select events //////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  
  @Override
  public boolean result(Object iRecord) {
    final ORecordAbstract<?> record = (ORecordAbstract<?>) iRecord;

    if (record.getIdentity().isValid()) {
      // RESET VERSION TO DISABLE MVCC AVOIDING THE CONCURRENT EXCEPTION IF LOCAL CACHE IS NOT UPDATED
      record.getRecordVersion().disable();
      record.delete();
      recordCount++;
      return true;
    }
    return false;
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
