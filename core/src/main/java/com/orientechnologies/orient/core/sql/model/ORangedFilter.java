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

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import static com.orientechnologies.orient.core.sql.model.OEquals.toStackPath;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public abstract class ORangedFilter extends OExpressionWithChildren {

  public ORangedFilter(List<OExpression> arguments) {
    super(arguments);
  }

  public ORangedFilter(String alias, List<OExpression> arguments) {
    super(alias, arguments);
  }

  public ORangedFilter(String alias, OExpression... children) {
    super(alias, children);
  }

  public OExpression getLeft() {
    return children.get(0);
  }

  public OExpression getRight() {
    return children.get(1);
  }

  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {

    final String className = searchContext.getSource().getTargetClasse();
    if (className == null) {
      //no optimisation
      return;
    }

    //test is equality match pattern : field(.subfiled)* <operator> value
    Map.Entry<List<OName>, OExpression> stack = toStackPath(getLeft(), getRight());
    if (stack == null) {
      //no optimisation
      return;
    }

    final List<OName> path = stack.getKey();
    OClass clazz = getDatabase().getMetadata().getSchema().getClass(className);
    final Map.Entry<List<OIndex>, OClass> indexUnfold = OPath.unfoldIndexes(path, clazz);
    if (indexUnfold == null) {
      return;
    }
    final List<OIndex> walk = indexUnfold.getKey();
    clazz = indexUnfold.getValue();

    final OName equalFieldName = path.get(path.size() - 1);
    final OExpression equalFieldValue = stack.getValue();
    final boolean found = analyzeSearchIndex(searchContext, result, clazz, equalFieldName, equalFieldValue);

    if (!found) {
      //could not find a proper index
      return;
    }

    //unfold the path
    OPath.foldIndexes(this, walk, searchResult);
  }

  /**
   *
   * @param searchContext
   * @param result
   * @param clazz
   * @param field
   * @param value
   * @return true if index use was found
   */
  protected abstract boolean analyzeSearchIndex(OSearchContext searchContext, OSearchResult result,
          OClass clazz, OName field, OExpression value);

  public static Map.Entry<List<OName>, OExpression> toStackPath(OExpression left, OExpression right) {

    if (right instanceof OPath || right instanceof OName) {
      //flip order, we want the path/Name on the left
      OExpression tmp = left;
      left = right;
      right = tmp;
    }

    if (!right.isStatic()) {
      //can't optimize with index
      return null;
    }

    final List<OName> path;
    if (left instanceof OName) {
      path = Collections.singletonList((OName) left);
    } else if (left instanceof OPath) {
      path = new ArrayList<OName>();
      final List<OExpression> unfolded = ((OPath) left).unfoldPath();
      for (OExpression e : unfolded) {
        if (e instanceof OName) {
          path.add((OName) e);
        } else {
          //can't optimize with index
          return null;
        }
      }
    } else {
      //can't optimize
      return null;
    }

    return new AbstractMap.SimpleImmutableEntry<List<OName>, OExpression>(path, right);
  }
}
