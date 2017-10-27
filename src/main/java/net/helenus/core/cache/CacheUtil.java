package net.helenus.core.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.helenus.core.reflect.MapExportable;

public class CacheUtil {

	public static List<String[]> combinations(List<String> items) {
		int n = items.size();
		if (n > 20 || n < 0)
			throw new IllegalArgumentException(n + " is out of range");
		long e = Math.round(Math.pow(2, n));
		List<String[]> out = new ArrayList<String[]>((int) e - 1);
		for (int k = 1; k <= items.size(); k++) {
			kCombinations(items, 0, k, new String[k], out);
		}
		return out;
	}

	private static void kCombinations(List<String> items, int n, int k, String[] arr, List<String[]> out) {
		if (k == 0) {
			out.add(arr.clone());
		} else {
			for (int i = n; i <= items.size() - k; i++) {
				arr[arr.length - k] = items.get(i);
				kCombinations(items, i + 1, k - 1, arr, out);
			}
		}
	}

	public static List<String[]> flattenFacets(List<Facet> facets) {
		List<String[]> combinations = CacheUtil.combinations(
				facets.stream().filter(facet -> !facet.fixed()).filter(facet -> facet.value() != null).map(facet -> {
					return facet.name() + "==" + facet.value();
				}).collect(Collectors.toList()));
		return combinations;
	}

	public static Object merge(Object to, Object from) {
		if (to == from) {
			return to;
		} else {
		    return from;
        }
/*
		// TODO(gburd): take ttl and writeTime into account when merging.
		Map<String, Object> toValueMap = to instanceof MapExportable ? ((MapExportable) to).toMap() : null;
		Map<String, Object> fromValueMap = to instanceof MapExportable ? ((MapExportable) from).toMap() : null;

		if (toValueMap != null && fromValueMap != null) {
			for (String key : fromValueMap.keySet()) {
				if (toValueMap.containsKey(key) && toValueMap.get(key) != fromValueMap.get(key)) {
					toValueMap.put(key, fromValueMap.get(key));
				}
			}
		}
		return to;
*/
	}

	public static String schemaName(List<Facet> facets) {
		return facets.stream().filter(Facet::fixed).map(facet -> facet.value().toString())
				.collect(Collectors.joining("."));
	}

}
