package net.helenus.core.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CacheUtil {

  public static List<String[]> combinations(List<String> items) {
    int n = items.size();
    if (n > 20) throw new IllegalArgumentException(n + " is out of range");
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

  public static List<String> flatKeys(String table, List<Facet> facets) {
    return flattenFacets(facets)
            .stream()
            .map(combination -> {
              return table + "." + Arrays.toString(combination);
            })
            .collect(Collectors.toList());
  }

  public static List<String[]> flattenFacets(List<Facet> facets) {
    List<String[]> combinations =
        CacheUtil.combinations(
            facets
                .stream()
                .filter(facet -> !facet.fixed())
                .filter(facet -> facet.value() != null)
                .map(
                    facet -> {
                      return facet.name() + "==" + facet.value();
                    })
                .collect(Collectors.toList()));
    // TODO(gburd): rework so as to not generate the combinations at all rather than filter
    for (Facet facet : facets) {
      if (facet.fixed()) continue;
      if (facet.alone() && facet.combined() && true) continue;
      combinations = combinations
              .stream()
              .filter(combo -> {
                for (String c : combo) {
                  // When used alone, this facet is not distinct so don't use it as a key.
                  if (facet.alone() == false && c.equals(facet.name())) {
                    return false;
                  }
                  // Don't use this facet in combination with others to create keys.
                  if (facet.combined() == false && c.split("==")[0].equals(facet.name())) {
                    return false;
                  }
                }
                return true;
              })
              .collect(Collectors.toList());
    }
    return combinations;
  }

  public static Object merge(Object to, Object from) {
    if (to == from) {
      return to;
    } else {
      return from;
    }
    /*
     * // TODO(gburd): take ttl and writeTime into account when merging. Map<String,
     * Object> toValueMap = to instanceof MapExportable ? ((MapExportable)
     * to).toMap() : null; Map<String, Object> fromValueMap = to instanceof
     * MapExportable ? ((MapExportable) from).toMap() : null;
     *
     * if (toValueMap != null && fromValueMap != null) { for (String key :
     * fromValueMap.keySet()) { if (toValueMap.containsKey(key) &&
     * toValueMap.get(key) != fromValueMap.get(key)) { toValueMap.put(key,
     * fromValueMap.get(key)); } } } return to;
     */
  }

  public static String schemaName(List<Facet> facets) {
    return facets
        .stream()
        .filter(Facet::fixed)
        .map(facet -> facet.value().toString())
        .collect(Collectors.joining("."));
  }

  public static String writeTimeKey(String propertyName) {
    return "_" + propertyName + "_writeTime";
  }

  public static String ttlKey(String propertyName) {
    return "_" + propertyName + "_ttl";
  }
}
