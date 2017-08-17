package net.helenus.core.operation;

import com.datastax.driver.core.ResultSet;

public interface Transformational<E> {
    E transform(ResultSet resultSet);
}
