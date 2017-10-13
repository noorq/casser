package net.helenus.core.reflect;

import java.util.Set;

public interface Drafted<T> extends MapExportable {

  Set<String> mutated();

  T build();
}
