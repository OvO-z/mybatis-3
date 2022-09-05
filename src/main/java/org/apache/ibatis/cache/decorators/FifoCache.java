/*
 *    Copyright 2009-2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.cache.decorators;

import java.util.Deque;
import java.util.LinkedList;

import org.apache.ibatis.cache.Cache;

/**
 * FIFO (first in, first out) cache decorator.<br/>
 * 目前 FifoCache 的逻辑实现上，有一定的问题，主要有两点。<br/>
 * <1> 处，如果重复添加一个缓存，那么在 keyList 里会存储两个，占用了缓存上限的两个名额。<br/>
 * <2> 处，在移除指定缓存时，不会移除 keyList 里占用的一个名额。<br/>
 * @author Clinton Begin
 */
public class FifoCache implements Cache {

  /**
   * 装饰的 Cache 对象
   */
  private final Cache delegate;

  /**
   * 双端队列，记录缓存键的添加
   */
  private final Deque<Object> keyList;

  /**
   * 队列上限
   */
  private int size;

  public FifoCache(Cache delegate) {
    this.delegate = delegate;
    // 使用了 LinkedList
    this.keyList = new LinkedList<>();
    // 默认为 1024
    this.size = 1024;
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  public void setSize(int size) {
    this.size = size;
  }

  @Override
  public void putObject(Object key, Object value) {
    // 循环 keyList
    cycleKeyList(key);
    delegate.putObject(key, value);
  }

  @Override
  public Object getObject(Object key) {
    return delegate.getObject(key);
  }

  @Override
  public Object removeObject(Object key) {
    return delegate.removeObject(key);
  }

  @Override
  public void clear() {
    delegate.clear();
    keyList.clear();
  }

  private void cycleKeyList(Object key) {
    // <1> 添加到 keyList 对位
    keyList.addLast(key);
    // 超过上限，将队首位移除
    if (keyList.size() > size) {
      Object oldestKey = keyList.removeFirst();
      delegate.removeObject(oldestKey);
    }
  }

}
