package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Returns the first <code>field/value</code> not null parameter. if no <code>field/value</code> is <b>not</b> null, returns null.
 * 
 * <p>
 * Syntax: <blockquote>
 * 
 * <pre>
 * coalesce(&lt;field|value&gt;[,&lt;field|value&gt;]*)
 * </pre>
 * 
 * </blockquote>
 * 
 * <p>
 * Examples: <blockquote>
 * 
 * <pre>
 * SELECT <b>coalesce('a', 'b')</b> FROM ...
 *  -> 'a'
 * 
 * SELECT <b>coalesce(null, 'b')</b> FROM ...
 *  -> 'b'
 * 
 * SELECT <b>coalesce(null, null, 'c')</b> FROM ...
 *  -> 'c'
 * 
 * SELECT <b>coalesce(null, null)</b> FROM ...
 *  -> null
 * 
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Claudio Tesoriero
 */

public class OSQLFunctionCoalesce extends OSQLFunctionAbstract {
  public static final String NAME = "coalesce";

  public OSQLFunctionCoalesce() {
    super(NAME, 1, 1000);
  }

  @Override
  public String getSyntax() {
    return "Returns the first not-null parameter or null if all parameters are null. Syntax: coalesce(<field|value> [,<field|value>]*)";
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    final int length = children.size();
    for (int i = 0; i < length; i++) {
      final Object obj = children.get(i).evaluate(context, candidate);
      if (obj != null)
        return obj;
    }
    return null;
  }
  
  @Override
  public OSQLFunctionCoalesce copy() {
    final OSQLFunctionCoalesce fct = new OSQLFunctionCoalesce();
    fct.setAlias(alias);
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}
