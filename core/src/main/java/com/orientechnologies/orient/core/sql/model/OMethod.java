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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OMethod extends OFunction{

  public OMethod(String name, OExpression source, List<OExpression> arguments) {
    this(name, null, source, arguments);
  }

  public OMethod(String name, String alias, OExpression source, List<OExpression> arguments) {
    super(name, alias, concat(source,arguments));
  }
  
  public OExpression getSource(){
    return getArguments().get(0);
  }
  
  public List<OExpression> getMethodArguments(){
    final List<OExpression> args = new ArrayList<OExpression>(getArguments());
    args.remove(0);
    return Collections.unmodifiableList(args);
  }
  
  private static List<OExpression> concat(OExpression source, List<OExpression> arguments){
    final List<OExpression> exps = new ArrayList<OExpression>(arguments.size()+1);
    exps.add(source);
    exps.addAll(arguments);
    return exps;
  }
  
  @Override
  protected String thisToString() {
    return "(Method) "+getName();
  }
  
}
