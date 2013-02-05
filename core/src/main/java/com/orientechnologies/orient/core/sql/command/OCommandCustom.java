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

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OCommandCustom extends OCommandExecutorSQLAbstract {

  private final List<Object> arguments;

  public OCommandCustom(List<Object> arguments) {
    this.arguments = arguments;
  }

  public List<Object> getArguments() {
    return arguments;
  }

  @Override
  public <RET extends OCommandExecutor> RET parse(OCommandRequest iRequest) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object execute(Map<Object, Object> iArgs) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
