package net.helenus.core;


@FunctionalInterface
public interface CommitThunk {
  void apply();
}
