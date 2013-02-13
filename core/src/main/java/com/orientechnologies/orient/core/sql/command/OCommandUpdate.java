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


import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandListener;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.parser.OSQLParser;
import static com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 * 
 */
public class OCommandUpdate extends OCommandAbstract implements OCommandListener {
  
  public static final String KEYWORD_UPDATE = "UPDATE";
  private static final OLiteral EMPTY_VALUE = new OLiteral(null);
  
  private OSQLParser.SourceContext source;
  private OSQLParser.FilterContext filter;
  private Map<String, OExpression>                setEntries        = new LinkedHashMap<String, OExpression>();
  private List<OPair<String, OExpression>>        addEntries        = new ArrayList<OPair<String, OExpression>>();
  private Map<String, OPair<String, OExpression>> putEntries        = new LinkedHashMap<String, OPair<String, OExpression>>();
  private List<OPair<String, OExpression>>        removeEntries     = new ArrayList<OPair<String, OExpression>>();
  private Map<String, OExpression>                incrementEntries  = new LinkedHashMap<String, OExpression>();
  private long recordCount = 0;
  
  public OCommandUpdate() {
  }

  public OCommandUpdate parse(final OCommandRequest iRequest) throws OCommandSQLParsingException {    
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    final OSQLParser.CommandUpdateContext candidate = getCommand(iRequest, OSQLParser.CommandUpdateContext.class);
    source = candidate.source();
    filter = candidate.filter();
    
    for(OSQLParser.UpdateGroupContext groupp : candidate.updateGroup()){
      if(groupp.updateSimpleGroup() != null){
        final OSQLParser.UpdateSimpleGroupContext group = groupp.updateSimpleGroup();
        if(group.SET() != null){
          for(OSQLParser.UpdateEntryContext entry : group.updateEntry()){
            setEntries.put(visitAsString(entry.reference()), visit(entry.expression()));
          }
        }else if(group.ADD() != null){
          for(OSQLParser.UpdateEntryContext entry : group.updateEntry()){
            addEntries.add(new OPair<String, OExpression>(visitAsString(entry.reference()), visit(entry.expression())));
          }
        }else if(group.REMOVE()!= null){
          for(OSQLParser.UpdateEntryContext entry : group.updateEntry()){
            OExpression value = EMPTY_VALUE;
            if(entry.expression()!= null){
              value = visit(entry.expression());
            }
            removeEntries.add(new OPair<String, OExpression>(visitAsString(entry.reference()),value));
          }
        }else if(group.INCREMENT()!= null){
          for(OSQLParser.UpdateEntryContext entry : group.updateEntry()){
            OExpression value = new OLiteral(0d);
            if(entry.expression() != null){
              value = visit(entry.expression());
            }
            incrementEntries.put(visitAsString(entry.reference()),value);
          }
        }
      }else if(groupp.updatePutGroup() != null){
        final OSQLParser.UpdatePutGroupContext group = groupp.updatePutGroup();
        for(OSQLParser.UpdatePutEntryContext entry : group.updatePutEntry()){
            final String fieldName = visitAsString(entry.reference(0));
            final String keyName = visitAsString(entry.reference(1));
            final OExpression value = visit(entry.expression());
            putEntries.put(fieldName, new OPair<String, OExpression>(keyName,value));
          }
      }
    }
    
    return this;
  }

  @Override
  public Object execute(final Map<Object, Object> iArgs) {
    recordCount = 0;
    final OCommandSelect subselect = new OCommandSelect();
    subselect.parse(source, filter);
    subselect.addListener(this);
    subselect.execute(iArgs);
    return recordCount;
  }

  @Override
  public String getSyntax() {
    return "UPDATE <class>|cluster:<cluster>> [SET|ADD|PUT|REMOVE|INCREMENT] [[,] <field-name> = <expression>|<sub-command>]* [WHERE <conditions>]";
  }

  //////////////////////////////////////////////////////////////////////////////
  //Sub select events //////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////
  
  @Override
  public boolean result(Object iRecord) {
    final ODocument record = (ODocument) ((OIdentifiable) iRecord).getRecord();
    final OCommandContext ctx = getContext();
    
    boolean recordUpdated = false;

    // BIND VALUES TO UPDATE
    if (!setEntries.isEmpty()) {
      for(Map.Entry<String,OExpression> entry : setEntries.entrySet()){
        record.field(entry.getKey(),entry.getValue().evaluate(ctx, record));
      }
      recordUpdated = true;
    }

    // BIND VALUES TO INCREMENT
    for (Map.Entry<String, OExpression> entry : incrementEntries.entrySet()) {
      final Number prevValue = record.field(entry.getKey());
      if (prevValue == null){
        // NO PREVIOUS VALUE: CONSIDER AS 0
        record.field(entry.getKey(), entry.getValue().evaluate(ctx, record));
      }else{
        // COMPUTING INCREMENT
        record.field(entry.getKey(), OType.increment(prevValue, (Number)entry.getValue().evaluate(context, record)));
      }
      recordUpdated = true;
    }
    
    Object v;

    // BIND VALUES TO ADD
    Collection<Object> coll;
    Object fieldValue;
    for (OPair<String, OExpression> entry : addEntries) {
      coll = null;
      if (!record.containsField(entry.getKey())) {
        // GET THE TYPE IF ANY
        if (record.getSchemaClass() != null) {
          OProperty prop = record.getSchemaClass().getProperty(entry.getKey());
          if (prop != null && prop.getType() == OType.LINKSET)
            // SET TYPE
            coll = new HashSet<Object>();
        }

        if (coll == null)
          // IN ALL OTHER CASES USE A LIST
          coll = new ArrayList<Object>();

        record.field(entry.getKey(), coll);
      } else {
        fieldValue = record.field(entry.getKey());

        if (fieldValue instanceof Collection<?>)
          coll = (Collection<Object>) fieldValue;
        else
          continue;
      }

      v = entry.getValue().evaluate(ctx, record);
      coll.add(v);
      recordUpdated = true;
    }

    // BIND VALUES TO PUT (AS MAP)
    Map<String, Object> map;
    OPair<String, OExpression> pair;
    for (Map.Entry<String, OPair<String, OExpression>> entry : putEntries.entrySet()) {
      fieldValue = record.field(entry.getKey());

      if (fieldValue == null) {
        if (record.getSchemaClass() != null) {
          final OProperty property = record.getSchemaClass().getProperty(entry.getKey());
          if (property != null
              && (property.getType() != null && (!property.getType().equals(OType.EMBEDDEDMAP) && !property.getType().equals(
                  OType.LINKMAP)))) {
            throw new OCommandExecutionException("field " + entry.getKey() + " is not defined as a map");
          }
        }
        fieldValue = new HashMap<String, Object>();
        record.field(entry.getKey(), fieldValue);
      }

      if (fieldValue instanceof Map<?, ?>) {
        map = (Map<String, Object>) fieldValue;
        pair = entry.getValue();

        v = pair.getValue().evaluate(ctx, record);

        map.put(pair.getKey(), v);
        recordUpdated = true;
      }
    }

    // REMOVE FIELD IF ANY
    for (OPair<String, OExpression> entry : removeEntries) {
      v = entry.getValue().evaluate(ctx, record);
      if (v == EMPTY_VALUE) {
        record.removeField(entry.getKey());
        recordUpdated = true;
      } else {
        fieldValue = record.field(entry.getKey());

        if (fieldValue instanceof Collection<?>) {
          coll = (Collection<Object>) fieldValue;
          if (coll.remove(v))
            recordUpdated = true;
        } else if (fieldValue instanceof Map<?, ?>) {
          map = (Map<String, Object>) fieldValue;
          if (map.remove(v) != null)
            recordUpdated = true;
        }
      }
    }

    if (recordUpdated) {
      record.setDirty();
      record.save();
      recordCount++;
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
