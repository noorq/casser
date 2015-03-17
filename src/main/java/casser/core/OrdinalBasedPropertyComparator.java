package casser.core;

import java.util.Comparator;

import casser.mapping.CasserMappingProperty;

public enum OrdinalBasedPropertyComparator implements Comparator<CasserMappingProperty<?>> {

	INSTANCE;


	public int compare(CasserMappingProperty<?> o1, CasserMappingProperty<?> o2) {

		Integer ordinal1 = o1.getOrdinal();
		Integer ordinal2 = o2.getOrdinal();

		if (ordinal1 == null) {
			if (ordinal2 == null) {
				return 0;
			}
			return -1;
		}

		if (ordinal2 == null) {
			return 1;
		}

		return ordinal1.compareTo(ordinal2);
	}

	
}
