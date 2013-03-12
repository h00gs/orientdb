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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OExpressionTraverse extends OExpressionAbstract {

  public static enum SOURCE {

    ANY, ALL
  }
  
  private Object source;
  private final List<OExpression> subfields = new ArrayList<OExpression>();
  private int startDepth;
  private int endDepth;
  private OExpression filter;

  public OExpressionTraverse() {
  }

  public Object getSource() {
    return source;
  }

  public void setSource(Object source) {
    this.source = source;
  }

  public List<OExpression> getSubfields() {
    return subfields;
  }

  public int getStartDepth() {
    return startDepth;
  }

  public void setStartDepth(int startDepth) {
    this.startDepth = startDepth;
  }

  public int getEndDepth() {
    return endDepth;
  }

  public void setEndDepth(int endDepth) {
    this.endDepth = endDepth;
  }

  public OExpression getFilter() {
    return filter;
  }

  public void setFilter(OExpression filter) {
    this.filter = filter;
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    final Collection candidates = valuesToTest(context, candidate, false);

    final Set visited = new HashSet();

    final int[] depth = new int[]{0};
    for (Object c : candidates) {
      final Boolean sr = evaluateSingle(context, c, depth, visited);
      if (Boolean.TRUE.equals(sr)) {
        return true;
      }
    }

    return false;
  }

  private Boolean evaluateSingle(OCommandContext context, Object candidate, int[] depth, Set visited) {
    if (visited.contains(candidate)) {
      //already tested
      return null;
    }
    visited.add(candidate);

    final Object eval = filter.evaluate(context, candidate);
    if (Boolean.TRUE.equals(eval)) {
      return true;
    }

    //test subs
    if (endDepth < 0 || depth[0] <= endDepth) {
      final Collection candidates = valuesToTest(context, candidate, true);
      depth[0]++;
      for (Object c : candidates) {
        final Boolean sr = evaluateSingle(context, c, depth, visited);
        if (Boolean.TRUE.equals(sr)) {
          return true;
        }
      }
      depth[0]--;
    }

    return null;
  }

  private Collection valuesToTest(OCommandContext context, Object candidate, boolean sub) {
    final Collection col = new ArrayList();
    if (candidate instanceof ORID) {
      candidate = ((ORID) candidate).getRecord();
    }
    if (candidate instanceof ODocument) {
      final ODocument doc = (ODocument) candidate;

      if (!sub) {
        if (source == SOURCE.ALL || source == SOURCE.ANY) {
          col.addAll(Arrays.asList(doc.fieldValues()));
        } else {
          final OExpression source = (OExpression) this.source;
          final Object obj = source.evaluate(context, candidate);
          if (obj != null) {
            col.add(obj);
          }
        }
      } else {
        if (subfields.isEmpty()) {
          col.addAll(Arrays.asList(doc.fieldValues()));
        } else {
          for (OExpression xp : subfields) {
            final Object obj = xp.evaluate(context, candidate);
            if (obj != null) {
              col.add(obj);
            }
          }
        }
      }

    } else if (candidate instanceof Map) {
      final Map map = (Map) candidate;

      if (!sub) {
        if (source == SOURCE.ALL || source == SOURCE.ANY) {
          col.addAll(map.values());
        } else {
          final OExpression source = (OExpression) this.source;
          final Object obj = source.evaluate(context, candidate);
          if (obj != null) {
            col.add(obj);
          }
        }
      } else {
        if (subfields.isEmpty()) {
          col.addAll(map.values());
        } else {
          for (OExpression xp : subfields) {
            final Object obj = xp.evaluate(context, candidate);
            if (obj != null) {
              col.add(obj);
            }
          }
        }
      }

    } else if (candidate instanceof Collection && sub) {
      col.addAll((Collection) candidate);
    }

    return col;
  }

  @Override
  public OExpressionTraverse copy() {
    final OExpressionTraverse cp = new OExpressionTraverse();
    cp.setAlias(getAlias());
    cp.setSource(getSource());
    cp.getSubfields().addAll(getSubfields());
    cp.setStartDepth(getStartDepth());
    cp.setEndDepth(getEndDepth());
    cp.setFilter(getFilter());
    return cp;
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
}
