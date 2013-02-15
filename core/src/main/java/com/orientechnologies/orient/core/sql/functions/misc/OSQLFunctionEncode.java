/*
 * Copyright 2013 Geomatys
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
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Encode a string in various format (only base64 for now)
 *
 * @author Johann Sorel (Geomatys)
 */
public class OSQLFunctionEncode extends OSQLFunctionAbstract {

  public static final String NAME = "encode";
  public static final String FORMAT_BASE64 = "base64";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public OSQLFunctionEncode() {
    super(NAME, 2, 2);
  }

  @Override
  public String getSyntax() {
    return "Syntax error: encode(<binaryfield>, <format>)";
  }

  @Override
  public OSQLFunctionEncode copy() {
    final OSQLFunctionEncode fct = new OSQLFunctionEncode();
    fct.setAlias(alias);
    fct.getArguments().addAll(getArguments());
    return fct;
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    
    final Object src = children.get(0).evaluate(context, candidate);
    final String format = children.get(1).evaluate(context, candidate).toString();

    byte[] data = null;
    if (src instanceof byte[]) {
      data = (byte[]) src;
    } else if (src instanceof ORecordId) {
      final ORecord rec = ((ORecordId) src).getRecord();
      if (rec instanceof ORecordBytes) {
        data = ((ORecordBytes) rec).toStream();
      }
    } else if (src instanceof OSerializableStream) {
      data = ((OSerializableStream) src).toStream();
    }

    if (data == null) {
      return null;
    }

    if (FORMAT_BASE64.equalsIgnoreCase(format)) {
      return OBase64Utils.encodeBytes(data);
    } else {
      throw new OException("unknowned format :" + format);
    }
  }
  
}
