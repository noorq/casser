/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.mapping.convert;

import com.google.common.base.CaseFormat;
import java.util.function.Function;

public enum CamelCaseToUnderscoreConverter implements Function<String, String> {
  INSTANCE;

  @Override
  public String apply(String source) {

    if (source == null) {
      throw new IllegalArgumentException("empty parameter");
    }

    return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, source);
  }
}
