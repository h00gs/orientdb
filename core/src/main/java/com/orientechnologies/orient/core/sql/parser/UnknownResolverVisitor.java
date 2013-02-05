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
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OUnset;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Replace ? expression by values from given parameter map.
 * @author Johann Sorel (Geomatys)
 */
public class UnknownResolverVisitor extends CopyVisitor{
  
  private final Iterator<Entry> entries;
  
  public UnknownResolverVisitor(Map parameters) {
    entries = parameters.entrySet().iterator();
  }

  @Override
  public Object visit(OUnset candidate, Object data) {
    if(!entries.hasNext()){
      throw new OException("Unmapped ? parameter");
    }
    final Object value = entries.next().getValue();
    return new OLiteral(value);
  }
  
}
