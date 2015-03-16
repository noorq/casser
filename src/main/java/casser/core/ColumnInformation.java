package casser.core;


public final class ColumnInformation {

	private final String columnName;
	
	public ColumnInformation(String columnName) {
		this.columnName = columnName;
	}

	public String getColumnName() {
		return columnName;
	}

	@Override
	public String toString() {
		return "ColumnInformation [columnName=" + columnName + "]";
	}
	
}
