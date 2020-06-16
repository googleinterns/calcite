/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.babel;

/**
 * Enumerates the types of WITH DATA clauses.
 */
public enum WithDataType {
  /**
   * The existence of whether data is included or not is not specified. Same behavior as NO_DATA.
   */
  UNSPECIFIED,

  /**
   * Table is created with the same schema and data.
   */
  WITH_DATA,

  /**
   * Table is explicitly created with the specified schema, but no data.
   */
  WITH_NO_DATA,
}
