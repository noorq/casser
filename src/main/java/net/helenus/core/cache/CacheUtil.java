package net.helenus.core.cache;

import net.helenus.core.Helenus;
import net.helenus.core.reflect.Entity;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.value.BeanColumnValueProvider;
import net.helenus.support.HelenusException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    facets = facets.stream()
            .filter(f -> !f.fixed())
            .filter(f -> !f.alone() || !f.combined())
            .collect(Collectors.toList());
    for (Facet facet : facets) {
      combinations = combinations
              .stream()
              .filter(combo -> {
                // When used alone, this facet is not distinct so don't use it as a key.
                if (combo.length == 1) {
                  if (!facet.alone() && combo[0].startsWith(facet.name() + "==")) {
                    return false;
                  }
                } else {
                  if (!facet.combined()) {
                    for (String c : combo) {
                      // Don't use this facet in combination with others to create keys.
                      if (c.startsWith(facet.name() + "==")) {
                        return false;
                      }
                    }
                  }
                }
                return true;
              })
              .collect(Collectors.toList());
    }
    return combinations;
  }

    /**
     * Merge changed values in the map behind `from` into `to`.
     */
  public static Object merge(Object t, Object f) {
      HelenusEntity entity = Helenus.resolve(MappingUtil.getMappingInterface(t));

      if (t == f) return t;
      if (f == null) return t;
      if (t == null) return f;

      if (t instanceof MapExportable && t instanceof Entity && f instanceof MapExportable && f instanceof Entity) {
          Entity to = (Entity) t;
          Entity from = (Entity) f;
          Map<String, Object> toValueMap = ((MapExportable) to).toMap();
          Map<String, Object> fromValueMap = ((MapExportable) from).toMap();
          for (HelenusProperty prop : entity.getOrderedProperties()) {
              switch (prop.getColumnType()) {
              case PARTITION_KEY:
              case CLUSTERING_COLUMN:
                  continue;
              default:
                  Object toVal = BeanColumnValueProvider.INSTANCE.getColumnValue(to, -1, prop, false);
                  Object fromVal = BeanColumnValueProvider.INSTANCE.getColumnValue(from, -1, prop, false);
                  String ttlKey = ttlKey(prop);
                  String writeTimeKey = writeTimeKey(prop);
                  int[] toTtlI = (int[]) toValueMap.get(ttlKey);
                  int toTtl = (toTtlI != null) ? toTtlI[0] : 0;
                  Long toWriteTime = (Long) toValueMap.get(writeTimeKey);
                  int[] fromTtlI = (int[]) fromValueMap.get(ttlKey);
                  int fromTtl = (fromTtlI != null) ? fromTtlI[0] : 0;
                  Long fromWriteTime = (Long) fromValueMap.get(writeTimeKey);

                  if (toVal != null) {
                      if (fromVal != null) {
                          if (toVal == fromVal) {
                              // Case: object identity
                              // Goal: ensure write time and ttl are also in sync
                              if (fromWriteTime != null && fromWriteTime != 0L &&
                                      (toWriteTime == null || fromWriteTime > toWriteTime)) {
                                  ((MapExportable) to).put(writeTimeKey, fromWriteTime);
                              }
                              if (fromTtl > 0 && fromTtl > toTtl) {
                                  ((MapExportable) to).put(ttlKey, fromTtl);
                              }
                          } else if (fromWriteTime != null && fromWriteTime != 0L) {
                              // Case: to exists and from exists
                              // Goal: copy over from -> to iff from.writeTime > to.writeTime
                              if (toWriteTime != null && toWriteTime != 0L) {
                                  if (fromWriteTime > toWriteTime) {
                                      ((MapExportable) to).put(prop.getPropertyName(), fromVal);
                                      ((MapExportable) to).put(writeTimeKey, fromWriteTime);
                                      if (fromTtl > 0) {
                                          ((MapExportable) to).put(ttlKey, fromTtl);
                                      }
                                  }
                              } else {
                                  ((MapExportable) to).put(prop.getPropertyName(), fromVal);
                                  ((MapExportable) to).put(writeTimeKey, fromWriteTime);
                                  if (fromTtl > 0) {
                                      ((MapExportable) to).put(ttlKey, fromTtl);
                                  }
                              }
                          } else {
                              if (toWriteTime == null || toWriteTime == 0L) {
                                  // Caution, entering grey area...
                                  if (!toVal.equals(fromVal)) {
                                      // dangerous waters here, values diverge without information that enables resolution,
                                      // policy (for now) is to move value from -> to anyway.
                                      ((MapExportable) to).put(prop.getPropertyName(), fromVal);
                                      if (fromTtl > 0) {
                                          ((MapExportable) to).put(ttlKey, fromTtl);
                                      }
                                  }
                              }
                          }
                      }
                  } else {
                      // Case: from exists, but to doesn't (it's null)
                      // Goal: copy over from -> to, include ttl and writeTime if present
                      if (fromVal != null) {
                          ((MapExportable) to).put(prop.getPropertyName(), fromVal);
                          if (fromWriteTime != null && fromWriteTime != 0L) {
                              ((MapExportable) to).put(writeTimeKey, fromWriteTime);
                          }
                          if (fromTtl > 0) {
                              ((MapExportable) to).put(ttlKey, fromTtl);
                          }
                      }
                  }
              }
          }
          return to;
      }
      return t;
  }

  public static String schemaName(List<Facet> facets) {
    return facets
        .stream()
        .filter(Facet::fixed)
        .map(facet -> facet.value().toString())
        .collect(Collectors.joining("."));
  }

  public static String writeTimeKey(HelenusProperty prop) {
      return writeTimeKey(prop.getColumnName().toCql(false));
  }

  public static String ttlKey(HelenusProperty prop) {
      return ttlKey(prop.getColumnName().toCql(false));
  }

  public static String writeTimeKey(String columnName) {
    return "_" + columnName + "_writeTime";
  }

  public static String ttlKey(String columnName) { return "_" + columnName + "_ttl"; }
}
