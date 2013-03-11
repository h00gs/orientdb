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

import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.model.*;
import com.orientechnologies.orient.core.sql.model.reflect.*;
import com.orientechnologies.orient.core.sql.operator.OSQLOperator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Johann Sorel (Geomatys)
 */
public class OToSQLVisitor implements OExpressionVisitor{

    private final boolean printAlias;
    private final boolean printIdEscape;
    
    private OToSQLVisitor(){
        this(false,true);
    }

    public OToSQLVisitor(boolean printAlias, boolean printIdEscape) {
        this.printAlias = printAlias;
        this.printIdEscape = printIdEscape;
    }

    private String visitAlias(String alias){
        if(!printAlias || alias == null){
            return "";
        }else{
            return " AS "+alias;
        }
    }

    @Override
    public String visit(OAnd candidate, Object data) {
      final StringBuilder sb = new StringBuilder("(");
      final List<OExpression> exps = candidate.getChildren();
      for(int i=0,n=exps.size();i<n;i++){
        sb.append(exps.get(i).accept(this,data));
        if(i<n-1){
          sb.append(" AND ");
        }
      }      
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String visit(OBetween candidate, Object data) {
        return  candidate.getTarget().accept(this,data)
                +" BETWEEN "+ candidate.getLeft().accept(this, data)
                +" AND "+ candidate.getRight().accept(this,data);
    }

    @Override
    public String visit(OCollection candidate, Object data) {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        final List<OExpression> lst = candidate.getChildren();
        int size = lst.size();
        for(int i=0;i<size;i++){
            final OExpression exp = lst.get(i);
            sb.append(exp.accept(this,data));
            if(i<size-1){
                sb.append(',');
            }
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String visit(OContextVariable candidate, Object data) {
        return "$"+candidate.getVariableName() + visitAlias(candidate.getAlias()) ;
    }

    @Override
    public String visit(OEquals candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" = "+ candidate.getRight().accept(this,data);
    }

    @Override
    public String visit(OExpression candidate, Object data) {
        throw new UnsupportedOperationException("Unknwoned expression : "+candidate);
    }

    @Override
    public String visit(OFiltered candidate, Object data) {
        return candidate.getSource().accept(this,data) +"["+ candidate.getFilter().accept(this, data)+"]"+visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OIn candidate, Object data) {
        return candidate.getLeft().accept(this, data) +" IN "+ candidate.getRight().accept(this,data);
    }

    @Override
    public String visit(OInferior candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" < "+ candidate.getRight().accept(this,data);
    }

    @Override
    public String visit(OInferiorEquals candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" <= "+ candidate.getRight().accept(this,data);
    }

    @Override
    public String visit(OIsNotNull candidate, Object data) {
        return candidate.getExpression().accept(this,data) +" IS NOT NULL ";
    }

    @Override
    public String visit(OIsNull candidate, Object data) {
        return candidate.getExpression().accept(this,data) +" IS NULL ";
    }

    @Override
    public String visit(OIsDefined candidate, Object data) {
        return candidate.getExpression().accept(this,data) +" IS DEFINED ";
    }

    @Override
    public String visit(OLike candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" LIKE "+ candidate.getRight().accept(this,data);
    }

    @Override
    public String visit(OLiteral candidate, Object data) {
        final Object value = candidate.getValue();
        if(value instanceof Number){
            return String.valueOf(candidate.getValue()) + visitAlias(candidate.getAlias());
        }else{
            return "'"+String.valueOf(candidate.getValue()) +"'"+ visitAlias(candidate.getAlias());
        }
    }

    @Override
    public String visit(OMap candidate, Object data) {
        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        final Set<Map.Entry<OLiteral,OExpression>> lst = candidate.getMap().entrySet();
        final Iterator<Map.Entry<OLiteral,OExpression>> ite = lst.iterator();
        int size = lst.size();
        for(int i=0;i<size;i++){
            ite.hasNext();
            final Map.Entry<OLiteral,OExpression> entry = ite.next();
            sb.append(entry.getKey().accept(this,data));
            sb.append(':');
            sb.append(entry.getValue().accept(this, data));
            if(i<size-1){
                sb.append(',');
            }
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String visit(OName candidate, Object data) {
        if(printIdEscape){
            return "\""+candidate.getName()+"\""+visitAlias(candidate.getAlias());
        }else{
            return candidate.getName()+visitAlias(candidate.getAlias());
        }
    }

    @Override
    public String visit(ONot candidate, Object data) {
        return " NOT "+candidate.getExpression().accept(this, data);
    }

    @Override
    public String visit(ONotEquals candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" <> "+ candidate.getRight().accept(this,data);
    }

    @Override
    public String visit(OOperatorDivide candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" / "+ candidate.getRight().accept(this,data)+visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OOperatorMinus candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" - "+ candidate.getRight().accept(this,data)+visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OOperatorModulo candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" % "+ candidate.getRight().accept(this,data)+visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OOperatorMultiply candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" * "+ candidate.getRight().accept(this,data)+visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OOperatorPlus candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" + "+ candidate.getRight().accept(this,data)+visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OOperatorPower candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" ^ "+ candidate.getRight().accept(this,data)+visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OOr candidate, Object data) {
        final StringBuilder sb = new StringBuilder("(");
      final List<OExpression> exps = candidate.getChildren();
      for(int i=0,n=exps.size();i<n;i++){
        sb.append(exps.get(i).accept(this,data));
        if(i<n-1){
          sb.append(" OR ");
        }
      }      
      sb.append(")");
      return sb.toString();
    }

    @Override
    public String visit(OPath candidate, Object data) {
        return candidate.getLeft().accept(this,data) +"."+ candidate.getRight().accept(this,data)+visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OSuperior candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" > "+ candidate.getRight().accept(this, data);
    }

    @Override
    public String visit(OSuperiorEquals candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" >= "+ candidate.getRight().accept(this,data);
    }

    @Override
    public String visit(OUnset candidate, Object data) {
        return "?";
    }

    @Override
    public String visit(OSQLFunction candidate, Object data) {
        final StringBuilder sb = new StringBuilder();
        sb.append(candidate.getName());
        sb.append("(");
        final List<OExpression> lst = candidate.getChildren();
        int size = lst.size();
        for(int i=0;i<size;i++){
            final OExpression exp = lst.get(i);
            sb.append(exp.accept(this,data));
            if(i<size-1){
                sb.append(',');
            }
        }
        sb.append(")");
        sb.append(visitAlias(candidate.getAlias()));
        return sb.toString();
    }

    @Override
    public String visit(OSQLMethod candidate, Object data) {
        final StringBuilder sb = new StringBuilder();
        sb.append(candidate.getSource().accept(this,data));
        sb.append(".");
        sb.append(candidate.getName());
        sb.append("(");
        final List<OExpression> lst = candidate.getMethodArguments();
        int size = lst.size();
        for(int i=0;i<size;i++){
            final OExpression exp = lst.get(i);
            sb.append(exp.accept(this,data));
            if(i<size-1){
                sb.append(',');
            }
        }
        sb.append(")");
        sb.append(visitAlias(candidate.getAlias()));
        return sb.toString();
    }

    @Override
    public String visit(OSQLOperator candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" "+candidate.getSyntax()+" "+ candidate.getRight().accept(this, data);
    }

    @Override
    public String visit(OExpressionClass candidate, Object data) {
        return "@class"+ visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OExpressionORID candidate, Object data) {
        return "@rid"+ visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OExpressionSize candidate, Object data) {
        return "@size"+ visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OExpressionThis candidate, Object data) {
        return "@this"+ visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OExpressionType candidate, Object data) {
        return "@type"+ visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OExpressionVersion candidate, Object data) {
        return "@version"+ visitAlias(candidate.getAlias());
    }

    @Override
    public Object visit(OExpressionRaw candidate, Object data) {
        return "@raw"+ visitAlias(candidate.getAlias());
    }
    
    @Override
    public String visit(OExpression.Include candidate, Object data) {
        return "1=1"+ visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OExpression.Exclude candidate, Object data) {
        return "1<>1"+ visitAlias(candidate.getAlias());
    }

    @Override
    public String visit(OExpression.Any candidate, Object data) {
        return "any()";
    }
  
    @Override
    public String visit(OExpression.All candidate, Object data) {
        return "all()";
    }
    @Override
    public Object visit(OExpressionTraverse candidate, Object data) {
        final Object source = candidate.getSource();
        final StringBuilder sb = new StringBuilder();
        if(source == OExpressionTraverse.SOURCE.ALL){
            sb.append("all()");
        }else if(source == OExpressionTraverse.SOURCE.ANY){
            sb.append("any()");
        }else{
            sb.append(((OExpression)source).accept(this, data));
        }
        
        sb.append(" traverse(");
        sb.append(candidate.getStartDepth());
        sb.append(",");
        sb.append(candidate.getEndDepth());
        for(OExpression ex : candidate.getSubfields()){
            sb.append(",");
            sb.append(ex.accept(this, data));
        }
        sb.append(") ");
        
        if(candidate.getFilter() != OExpression.INCLUDE){
            sb.append(" (");
            sb.append(candidate.getFilter().accept(this, data));
            sb.append(" )");
        }
        
        return sb.toString();
    }

    @Override
    public Object visit(OInstanceOf candidate, Object data) {
        return candidate.getLeft().accept(this,data) +" instanceof "+ candidate.getRight().accept(this,data);
    }
}
