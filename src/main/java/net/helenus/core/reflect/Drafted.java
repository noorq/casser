package net.helenus.core.reflect;

import net.helenus.mapping.HelenusEntity;

import java.util.Set;

public interface Drafted<T> extends MapExportable {

  Set<String> mutated();

  T build();
}
