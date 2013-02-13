/*
 * Copyright 2013 Orient Technologies.
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
package com.orientechnologies.orient.core.sql.command;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandListener;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import java.util.Map;
import javax.swing.event.EventListenerList;

/**
 *
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 */
public abstract class OCommandAbstract implements OCommandExecutor {

  private final EventListenerList listeners = new EventListenerList();
  private ProgressListenerWrap progressListener;
  protected int limit = -1;
  protected Map<Object, Object> parameters;
  protected OCommandContext context;

  public String getSyntax() {
    return "?";
  }
  
  public <RET extends OCommandExecutor> RET setProgressListener(OProgressListener progressListener) {
    removeListener(this.progressListener);
    if(progressListener != null){
      this.progressListener = new ProgressListenerWrap(progressListener);
      addListener(this.progressListener);
    }
    return (RET) this;
  }

  public int getLimit() {
    return limit;
  }

  public <RET extends OCommandExecutor> RET setLimit(final int iLimit) {
    this.limit = iLimit;
    return (RET) this;
  }

  public Map<Object, Object> getParameters() {
    return parameters;
  }

  public boolean isIdempotent() {
    return false;
  }
  
  public OCommandContext getContext() {
    if (context == null)
      context = new OBasicCommandContext();
    return context;
  }

  protected ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
  
  @Override
  public void addListener(OCommandListener listener) {
    listeners.add(OCommandListener.class, listener);
  }

  @Override
  public void removeListener(OCommandListener listener) {
    listeners.remove(OCommandListener.class, listener);
  }
  
  protected void fireBegin(Object itask, long iTotal){
    final OCommandListener[] lst = listeners.getListeners(OCommandListener.class);
    for(OCommandListener l : lst){
      l.onBegin(itask, iTotal);
    }
  }
  
  protected boolean fireProgress(Object itask, long iCounter, float iPercent){
    boolean continu = true;
    final OCommandListener[] lst = listeners.getListeners(OCommandListener.class);
    for(OCommandListener l : lst){
      continu &= l.onProgress(itask, iCounter, iPercent);
    }
    return continu;
  }
  
  protected void fireCompletition(Object itask, boolean iSuceed){
    final OCommandListener[] lst = listeners.getListeners(OCommandListener.class);
    for(OCommandListener l : lst){
      l.onCompletition(itask, iSuceed);
    }
  }
  
  protected boolean fireResult(Object iResult){
    boolean continu = true;
    final OCommandListener[] lst = listeners.getListeners(OCommandListener.class);
    for(OCommandListener l : lst){
       continu &= l.result(iResult);
    }
    return continu;
  }
  
  protected void fireEnd(){
    final OCommandListener[] lst = listeners.getListeners(OCommandListener.class);
    for(OCommandListener l : lst){
      l.end();
    }
  }
  
  protected static class ProgressListenerWrap implements OCommandListener{

    private final OProgressListener sub;

    public ProgressListenerWrap(OProgressListener sub) {
      this.sub = sub;
    }
    
    @Override
    public boolean result(Object iRecord) {
      return true;
    }

    @Override
    public void end() {
    }

    @Override
    public void onBegin(Object iTask, long iTotal) {
      sub.onBegin(iTask, iTotal);
    }

    @Override
    public boolean onProgress(Object iTask, long iCounter, float iPercent) {
      return sub.onProgress(iTask, iCounter, iPercent);
    }

    @Override
    public void onCompletition(Object iTask, boolean iSucceed) {
      sub.onCompletition(iTask, iSucceed);
    }
    
  }
  
  protected static class ResultListenerWrap implements OCommandListener{

    private final OCommandResultListener sub;

    public ResultListenerWrap(OCommandResultListener sub) {
      this.sub = sub;
    }
    
    @Override
    public boolean result(Object iRecord) {
      return sub.result(iRecord);
    }

    @Override
    public void end() {
      sub.end();
    }

    @Override
    public void onBegin(Object iTask, long iTotal) {
    }

    @Override
    public boolean onProgress(Object iTask, long iCounter, float iPercent) {
      return true;
    }

    @Override
    public void onCompletition(Object iTask, boolean iSucceed) {
    }
    
  }
}
