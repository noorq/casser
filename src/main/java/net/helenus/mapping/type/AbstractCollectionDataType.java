package net.helenus.mapping.type;

import net.helenus.mapping.ColumnType;

public abstract class AbstractCollectionDataType extends AbstractDataType {

	public AbstractCollectionDataType(ColumnType columnType) {
		super(columnType);
	}

	public boolean isCollectionType() {
		return true;
	}
}
