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
package org.apache.calcite.rel.type;

/**
 * Represents the record type for unknown tables (whose type information is not
 * in the pre-defined schema). This object allows us to treat unknown tables as
 * if they are dynamic tables (unknown number of columns with unknown types),
 * and is only generated when the {@code SqlValidator.Config.allowUnknownTables}
 * option is enabled.
 */
public class UnknownRecordType extends DynamicRecordTypeImpl {
  public UnknownRecordType(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }
}
