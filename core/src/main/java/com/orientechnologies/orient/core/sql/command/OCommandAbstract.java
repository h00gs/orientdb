/*
 * Copyright 2013 Orient Technologies.
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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import java.util.Map;

/**
 *
 * @author jsorel
 */
public abstract class OCommandAbstract implements OCommand {
  
  public static final String KEYWORD_FROM      = "FROM";
  public static final String KEYWORD_LET       = "LET";
  public static final String KEYWORD_WHERE     = "WHERE";
  public static final String KEYWORD_LIMIT     = "LIMIT";
  public static final String KEYWORD_SKIP      = "SKIP";
  public static final String KEYWORD_KEY       = "key";
  public static final String KEYWORD_RID       = "rid";
  public static final String CLUSTER_PREFIX    = "CLUSTER:";
  public static final String CLASS_PREFIX      = "CLASS:";
  public static final String INDEX_PREFIX      = "INDEX:";
  public static final String DICTIONARY_PREFIX = "DICTIONARY:";

  
  
  public static ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }


  @Override
  public <RET extends OCommandExecutor> RET parse(OCommandRequest iRequest) {
    //do nothing
    return (RET) this;
  }

  @Override
  public <RET extends OCommandExecutor> RET setProgressListener(OProgressListener progressListener) {
    //do nothing
    return (RET) this;
  }

  @Override
  public <RET extends OCommandExecutor> RET setLimit(int iLimit) {
    //do nothing
    return (RET) this;
  }

  @Override
  public Map<Object, Object> getParameters() {
    //do nothing
    return null;
  }

  @Override
  public OCommandContext getContext() {
    //do nothing
    return null;
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }
  
}
