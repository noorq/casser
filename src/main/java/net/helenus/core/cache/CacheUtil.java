package net.helenus.core.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CacheUtil {
    public static List<String[]> comb(String... items) {
        int n = items.length;
        if (n > 20 || n < 0) throw new IllegalArgumentException(n + " is out of range");
        long e = Math.round(Math.pow(2, n));
        List<String[]> out = new ArrayList<String[]>((int) e - 1);
        Arrays.sort(items);
        for (int k = 1; k <= items.length; k++) {
            kcomb(items, 0, k, new String[k], out);
        }
        return out;
    }

    private static void kcomb(String[] items, int n, int k, String[] arr, List<String[]> out) {
        if (k == 0) {
            out.add(arr.clone());
        } else {
            for (int i = n; i <= items.length - k; i++) {
                arr[arr.length - k] = items[i];
                kcomb(items, i + 1, k - 1, arr, out);
            }
        }
    }
}
