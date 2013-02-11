/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

/**
 * SQL GRANT command: Grant a privilege to a database role.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OCommandPermissionAbstract extends OCommandAbstract {
  protected static final String KEYWORD_ON = "ON";
  protected int                 privilege;
  protected String              resource;
  protected ORole               role;

  protected void parsePrivilege(final String privilegeName) {

    if ("CREATE".equalsIgnoreCase(privilegeName))
      privilege = ORole.PERMISSION_CREATE;
    else if ("READ".equalsIgnoreCase(privilegeName))
      privilege = ORole.PERMISSION_READ;
    else if ("UPDATE".equalsIgnoreCase(privilegeName))
      privilege = ORole.PERMISSION_UPDATE;
    else if ("DELETE".equalsIgnoreCase(privilegeName))
      privilege = ORole.PERMISSION_DELETE;
    else if ("ALL".equalsIgnoreCase(privilegeName))
      privilege = ORole.PERMISSION_ALL;
    else if ("NONE".equalsIgnoreCase(privilegeName))
      privilege = ORole.PERMISSION_NONE;
    else
      throw new OCommandSQLParsingException("Unrecognized privilege '" + privilegeName + "'");
  }

}
