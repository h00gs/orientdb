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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.model.OExpression;

/**
 * CONTAINS ALL operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLOperatorContainsAll extends OSQLOperator {
  
  public static final String NAME = "CONTAINSALL";

  public OSQLOperatorContainsAll() {
    super(NAME);
  }

  public OSQLOperatorContainsAll(OExpression left, OExpression right) {
		super(NAME, left, right);
	}

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    throw new UnsupportedOperationException("Not supported yet.");
    
//		final OSQLFilterCondition condition;
//
//		if (iCondition.getLeft() instanceof OSQLFilterCondition)
//			condition = (OSQLFilterCondition) iCondition.getLeft();
//		else if (iCondition.getRight() instanceof OSQLFilterCondition)
//			condition = (OSQLFilterCondition) iCondition.getRight();
//		else
//			condition = null;
//
//		if (iLeft.getClass().isArray()) {
//			if (iRight.getClass().isArray()) {
//				// ARRAY VS ARRAY
//				int matches = 0;
//				for (final Object l : (Object[]) iLeft) {
//					for (final Object r : (Object[]) iRight) {
//						if (OQueryOperatorEquals.equals(l, r)) {
//							++matches;
//							break;
//						}
//					}
//				}
//				return matches == ((Object[]) iRight).length;
//			} else if (iRight instanceof Collection<?>) {
//				// ARRAY VS ARRAY
//				int matches = 0;
//				for (final Object l : (Object[]) iLeft) {
//					for (final Object r : (Collection<?>) iRight) {
//						if (OQueryOperatorEquals.equals(l, r)) {
//							++matches;
//							break;
//						}
//					}
//				}
//				return matches == ((Collection<?>) iRight).size();
//			}
//
//		} else if (iLeft instanceof Collection<?>) {
//
//			final Collection<ORecordSchemaAware<?>> collection = (Collection<ORecordSchemaAware<?>>) iLeft;
//
//			if (condition != null) {
//				// CHECK AGAINST A CONDITION
//				for (final ORecordSchemaAware<?> o : collection) {
//					if ((Boolean) condition.evaluate(o, null, iContext) == Boolean.FALSE)
//						return false;
//				}
//			} else {
//				// CHECK AGAINST A SINGLE VALUE
//				for (final Object o : collection) {
//					if (!OQueryOperatorEquals.equals(iRight, o))
//						return false;
//				}
//			}
//		} else if (iRight instanceof Collection<?>) {
//
//			// CHECK AGAINST A CONDITION
//			final Collection<ORecordSchemaAware<?>> collection = (Collection<ORecordSchemaAware<?>>) iRight;
//
//			if (condition != null) {
//				for (final ORecordSchemaAware<?> o : collection) {
//					if ((Boolean) condition.evaluate(o, null, iContext) == Boolean.FALSE)
//						return false;
//				}
//			} else {
//				// CHECK AGAINST A SINGLE VALUE
//				for (final Object o : collection) {
//					if (!OQueryOperatorEquals.equals(iLeft, o))
//						return false;
//				}
//			}
//		}
//		return true;
	}

  @Override
  public OSQLOperatorContainsAll copy() {
    final OSQLOperatorContainsAll cp = new OSQLOperatorContainsAll();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }

}
