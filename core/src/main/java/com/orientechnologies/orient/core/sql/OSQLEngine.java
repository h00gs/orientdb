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
package com.orientechnologies.orient.core.sql;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLTarget;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;

public class OSQLEngine {

	private static Set<OSQLFunctionFactory>					FUNCTION_FACTORIES    = null;
	private static Set<OCommandExecutorSQLFactory>	EXECUTOR_FACTORIES	  = null;
  
	protected static final OSQLEngine								INSTANCE						= new OSQLEngine();
  
	private static ClassLoader											orientClassLoader		= OSQLEngine.class.getClassLoader();

	protected OSQLEngine() {
	}

	public void registerFunction(final String iName, final OSQLFunction iFunction) {
		ODynamicSQLElementFactory.FUNCTIONS.put(iName.toUpperCase(Locale.ENGLISH), iFunction);
	}

	public void registerFunction(final String iName, final Class<? extends OSQLFunction> iFunctionClass) {
		ODynamicSQLElementFactory.FUNCTIONS.put(iName.toUpperCase(Locale.ENGLISH), iFunctionClass);
	}

	public OSQLFunction getFunction(String iFunctionName) {
		iFunctionName = iFunctionName.toUpperCase(Locale.ENGLISH);

		final Iterator<OSQLFunctionFactory> ite = getFunctionFactories();
		while (ite.hasNext()) {
			final OSQLFunctionFactory factory = ite.next();
			if (factory.hasFunction(iFunctionName)) {
				return factory.createFunction(iFunctionName);
			}
		}

		throw new OCommandSQLParsingException("No function for name " + iFunctionName + ", available names are : "
				+ OCollections.toString(getFunctionNames()));
	}

	public void unregisterFunction(String iName) {
		iName = iName.toUpperCase(Locale.ENGLISH);
		ODynamicSQLElementFactory.FUNCTIONS.remove(iName);
	}

	/**
	 * @return Iterator of all function factories
	 */
	public static synchronized Iterator<OSQLFunctionFactory> getFunctionFactories() {
		if (FUNCTION_FACTORIES == null) {

			final Iterator<OSQLFunctionFactory> ite = lookupProviderWithOrientClassLoader(OSQLFunctionFactory.class, orientClassLoader);

			final Set<OSQLFunctionFactory> factories = new HashSet<OSQLFunctionFactory>();
			while (ite.hasNext()) {
				factories.add(ite.next());
			}
			FUNCTION_FACTORIES = Collections.unmodifiableSet(factories);
		}
		return FUNCTION_FACTORIES.iterator();
	}

  /**
	 * @return Iterator of all command factories
	 */
	public static synchronized Iterator<OCommandExecutorSQLFactory> getCommandFactories() {
		if (EXECUTOR_FACTORIES == null) {

			final Iterator<OCommandExecutorSQLFactory> ite = lookupProviderWithOrientClassLoader(OCommandExecutorSQLFactory.class,
					orientClassLoader);
			final Set<OCommandExecutorSQLFactory> factories = new HashSet<OCommandExecutorSQLFactory>();
			while (ite.hasNext()) {
				try {
					factories.add(ite.next());
				} catch (Exception e) {
					OLogManager.instance().warn(null, "Cannot load OCommandExecutorSQLFactory instance from service registry", e);
				}
			}

			EXECUTOR_FACTORIES = Collections.unmodifiableSet(factories);
      
		}
		return EXECUTOR_FACTORIES.iterator();
	}

	/**
	 * Iterates on all factories and append all function names.
	 * 
	 * @return Set of all function names.
	 */
	public static Set<String> getFunctionNames() {
		final Set<String> types = new HashSet<String>();
		final Iterator<OSQLFunctionFactory> ite = getFunctionFactories();
		while (ite.hasNext()) {
			types.addAll(ite.next().getFunctionNames());
		}
		return types;
	}

	/**
	 * Iterates on all factories and append all command names.
	 * 
	 * @return Set of all command names.
	 */
	public static Set<String> getCommandNames() {
		final Set<String> types = new HashSet<String>();
		final Iterator<OCommandExecutorSQLFactory> ite = getCommandFactories();
		while (ite.hasNext()) {
			types.addAll(ite.next().getCommandNames());
		}
		return types;
	}

	/**
	 * Scans for factory plug-ins on the application class path. This method is needed because the application class path can
	 * theoretically change, or additional plug-ins may become available. Rather than re-scanning the classpath on every invocation of
	 * the API, the class path is scanned automatically only on the first invocation. Clients can call this method to prompt a
	 * re-scan. Thus this method need only be invoked by sophisticated applications which dynamically make new plug-ins available at
	 * runtime.
	 */
	public static synchronized void scanForPlugins() {
		// clear cache, will cause a rescan on next getFunctionFactories call
		FUNCTION_FACTORIES = null;
	}

	public OCommandExecutor getCommand(final String candidate) {
		final Set<String> names = getCommandNames();
    
    //find the max command name size, to avoid searching for too long
    int maxSize = 0;
    for(String name : names){
      maxSize = Math.max(maxSize, name.length());
    }
    
		String commandName = candidate;
		boolean found = names.contains(commandName);
		int pos = -1;
		while (!found) {
			pos = OStringSerializerHelper.getLowerIndexOf(candidate, pos + 1, " ", "\n", "\r");
			if (pos > -1 && pos <= maxSize) {
        commandName = candidate.substring(0, pos).toUpperCase(Locale.ENGLISH);
        found = names.contains(commandName);
      } else {
        break;
      }
		}

		if (found) {
			final Iterator<OCommandExecutorSQLFactory> ite = getCommandFactories();
			while (ite.hasNext()) {
				final OCommandExecutorSQLFactory factory = ite.next();
				if (factory.getCommandNames().contains(commandName)) {
					return factory.createCommand(commandName);
				}
			}
		}

		return null;
	}

	public OSQLFilter parseCondition(final String iText, final OCommandContext iContext, final String iFilterKeyword) {
		return new OSQLFilter(iText, iContext, iFilterKeyword);
	}

	public OSQLTarget parseTarget(final String iText, final OCommandContext iContext, final String iFilterKeyword) {
		return new OSQLTarget(iText, iContext, iFilterKeyword);
	}

	public static OSQLEngine getInstance() {
		return INSTANCE;
	}

}
