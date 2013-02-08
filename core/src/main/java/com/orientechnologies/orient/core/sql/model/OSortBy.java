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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OSortBy {
  
  public enum Direction{
    ASC,
    DESC
  }
  
  private final OExpression exp;
  private final Direction direction;

  public OSortBy(OExpression exp, Direction direction) {
    this.exp = exp;
    this.direction = direction;
  }

  public OExpression getExpression() {
    return exp;
  }

  public Direction getDirection() {
    return direction;
  }
  
  public Comparator createComparator(final OCommandContext context){
    Comparator c = new OExpressionComparator(context, this.exp);
    if(direction == Direction.DESC){
      c = Collections.reverseOrder(c);
    }
    return c;
  }
  
  private static class OExpressionComparator implements Comparator{

    private final OCommandContext context;
    private final OExpression exp;

    public OExpressionComparator(final OCommandContext context, final OExpression exp) {
      this.context = context;
      this.exp = exp;
    }
    
    @Override
    public int compare(Object o1, Object o2) {
      final Object left = exp.evaluate(context, o1);
      final Object right = exp.evaluate(context, o2);
      
      if(left == null && right == null){
        return 0;
      }
      
      if(left == null){
        return -1;
      }else if(right == null){
        return +1;
      }
      
      if(left instanceof Number && right instanceof Number){
        final Double dl = (Double)((Number)left).doubleValue();
        final Double dr = (Double)((Number)right).doubleValue();
        return dl.compareTo(dr);
      }
      
      int res = 0;
      if(left instanceof Comparator && right instanceof Comparable){
        try{
          res = ((Comparable)left).compareTo(right);
        }catch(Exception ex){ /* we tryed */ }
      }
      
      //not comparable
      return 0;
    }
  
  }
  
  public static Comparator createComparator(final OCommandContext context, final List<OSortBy> sortBys){
    final int size = sortBys.size();
    if(size == 0){
      throw new IllegalArgumentException("Must pass in at least one sort by");
    }else if(size == 1){
      return sortBys.get(0).createComparator(context);
    }else{
      final Comparator[] stack = new Comparator[size];
      for(int i=0;i<size;i++){
        stack[i] = sortBys.get(i).createComparator(context);
      }
      return new Comparator() {
        @Override
        public int compare(Object o1, Object o2) {
          int res = 0;
          for(Comparator c : stack){
            res = c.compare(o1, o2);
            if(res != 0) break;
          }
          return res;
        }
      };
    }
  }
  
}
