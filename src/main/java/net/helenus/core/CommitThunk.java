package net.helenus.core;

import java.util.function.Function;

@FunctionalInterface
public interface CommitThunk {
    void apply();
}
