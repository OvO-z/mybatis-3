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
package org.apache.ibatis.parsing;

/**
 * #mark- 通用的 Token 解析器 - token就是占位符
 * 完成对字符串中${}和#{}的内容定位，每次定位完成后，调用{@link TokenHandler}进行内容替换。
 *
 * @author Clinton Begin
 */
public class GenericTokenParser {

  /**
   * 开始的 Token 字符串
   */
  private final String openToken;

  /**
   * 结束的 Token 字符串
   */
  private final String closeToken;
  private final TokenHandler handler;

  public GenericTokenParser(String openToken, String closeToken, TokenHandler handler) {
    this.openToken = openToken;
    this.closeToken = closeToken;
    this.handler = handler;
  }

  /**
   * 这段代码主要处理{@code text}拥有多个符合以{@code openToken}开头，{@code closeToken}结尾的字符串的情况
   * 同时还要处理拥有{@code openToken}或{@code closeToken}，但是使用了转义字符的情况。
   * @param text
   * @return
   */
  public String parse(String text) {
    if (text == null || text.isEmpty()) {
      return "";
    }
    // search open token
    // <1> 寻找开始的 openToken 的位置
    int start = text.indexOf(openToken);
    if (start == -1) {
      // 找不到直接返回
      return text;
    }
    char[] src = text.toCharArray();
    // 结果
    int offset = 0;
    final StringBuilder builder = new StringBuilder();
    // 匹配到 openToken 和 closeToken 之间的表达式
    StringBuilder expression = null;
    // 循环匹配
    do {
      // 转义字符
      if (start > 0 && src[start - 1] == '\\') {
        // this open token is escaped. remove the backslash and continue.
        // 因为 openToken 前面一个位置是 \ 转义字符，所以忽略 \
        // 添加 [offset, start - offset - 1] 和 openToken 的内容，添加到 builder 中
        builder.append(src, offset, start - offset - 1).append(openToken);
        // 修改 offset
        offset = start + openToken.length();
        // 非转义字符
      } else {
        // found open token. let's search close token.
        // 创建/重置 expression 对象
        if (expression == null) {
          expression = new StringBuilder();
        } else {
          expression.setLength(0);
        }
        // 添加 offset 和 openToken 之间的内容，添加到 builder 中
        builder.append(src, offset, start - offset);
        // 修改 offset
        offset = start + openToken.length();
        // 寻找结束的 closeToken 的位置
        int end = text.indexOf(closeToken, offset);
        while (end > -1) {
          // 转义
          if (end > offset && src[end - 1] == '\\') {
            // this close token is escaped. remove the backslash and continue.
            // 因为 endToken 前面一个位置是 \ 转义字符，所以忽略 \
            // 添加 [offset, end - offset - 1] 和 endToken 的内容，添加到 builder 中
            expression.append(src, offset, end - offset - 1).append(closeToken);
            // 继续，寻找结束的 closeToken 的位置
            offset = end + closeToken.length();
            end = text.indexOf(closeToken, offset);
          } else {
            // 添加 [offset, end - offset] 的内容，添加到 builder 中
            expression.append(src, offset, end - offset);
            break;
          }
        }
        // 拼接内容
        if (end == -1) {
          // close token was not found.
          // closeToken 未找到，直接拼接
          builder.append(src, start, src.length - start);
          offset = src.length;
        } else {
          builder.append(handler.handleToken(expression.toString()));
          offset = end + closeToken.length();
        }
      }
      start = text.indexOf(openToken, offset);
    } while (start > -1);
    if (offset < src.length) {
      builder.append(src, offset, src.length - offset);
    }
    return builder.toString();
  }
}
