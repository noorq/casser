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
package net.helenus.mapping.type;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.schemabuilder.*;
import java.util.List;
import net.helenus.mapping.ColumnType;
import net.helenus.mapping.IdentityName;
import net.helenus.support.HelenusMappingException;

public final class UDTKeyMapDataType extends AbstractCollectionDataType {

  private final IdentityName keyType;
  private final Class<?> udtKeyClass;
  private final DataType valueType;

  public UDTKeyMapDataType(
      ColumnType columnType, IdentityName keyType, Class<?> udtKeyClass, DataType valueType) {
    super(columnType);
    this.keyType = keyType;
    this.udtKeyClass = udtKeyClass;
    this.valueType = valueType;
  }

  @Override
  public Class<?>[] getTypeArguments() {
    return new Class<?>[] {udtKeyClass};
  }

  public IdentityName getUdtKeyName() {
    return keyType;
  }

  public Class<?> getUdtKeyClass() {
    return udtKeyClass;
  }

  @Override
  public void addColumn(Create create, IdentityName columnName) {
    ensureSimpleColumn(columnName);

    UDTType keyUdtType = SchemaBuilder.frozen(keyType.toCql());
    create.addUDTMapColumn(columnName.toCql(), keyUdtType, valueType);
  }

  @Override
  public void addColumn(CreateType create, IdentityName columnName) {
    ensureSimpleColumn(columnName);

    UDTType keyUdtType = SchemaBuilder.frozen(keyType.toCql());
    create.addUDTMapColumn(columnName.toCql(), keyUdtType, valueType);
  }

  @Override
  public SchemaStatement alterColumn(
      Alter alter, IdentityName columnName, OptionalColumnMetadata columnMetadata) {

    if (columnMetadata == null) {
      return notSupportedOperation("add", columnName);
    }

    DataType schemaDataType = columnMetadata.getType();
    if (schemaDataType.getName() != DataType.Name.MAP) {
      return notSupportedOperation("alter", columnName);
    }

    List<DataType> args = columnMetadata.getType().getTypeArguments();
    if (args.size() != 2 || !args.get(1).equals(valueType)) {
      return notSupportedOperation("alter", columnName);
    }

    DataType keyDataType = args.get(0);
    if (keyDataType.getName() != DataType.Name.UDT || !(keyDataType instanceof UserType)) {
      return notSupportedOperation("alter", columnName);
    }

    UserType udtKeyType = (UserType) keyDataType;

    if (!keyType.getName().equals(udtKeyType.getTypeName())) {
      return notSupportedOperation("alter", columnName);
    }

    // equals
    return null;
  }

  private SchemaStatement notSupportedOperation(String op, IdentityName columnName) {
    throw new HelenusMappingException(
        op + " UDTMap column is not supported by Cassandra Driver for column '" + columnName + "'");
  }

  @Override
  public String toString() {
    return "UDTKeyMap<" + keyType + "," + valueType + ">";
  }
}
