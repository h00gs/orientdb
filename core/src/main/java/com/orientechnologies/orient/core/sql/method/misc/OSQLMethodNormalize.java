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
package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.model.OExpression;
import java.text.Normalizer;
import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodNormalize extends OSQLMethod {

  public static final String NAME = "normalize";

  public OSQLMethodNormalize() {
    super(NAME, 0, 2);
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    final List<OExpression> arguments = getMethodArguments();
    Object value = getSource().evaluate(context, candidate);
    if (value != null) {
      final Normalizer.Form form = arguments.size() > 0 ? Normalizer.Form
              .valueOf(OStringSerializerHelper.getStringContent(arguments.get(0).evaluate(context, candidate).toString())) : Normalizer.Form.NFD;

      String normalized = Normalizer.normalize(value.toString(), form);
      if (arguments.size() > 1) {
        normalized = normalized.replaceAll(OStringSerializerHelper.getStringContent(arguments.get(0).evaluate(context, candidate).toString()), "");
      } else {
        normalized = normalized.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
      }
      value = normalized;
    }
    return value;
  }
  
  @Override
  public OSQLMethodNormalize copy() {
    final OSQLMethodNormalize method = new OSQLMethodNormalize();
    method.getArguments().addAll(getArguments());
    return method;
  }
}
