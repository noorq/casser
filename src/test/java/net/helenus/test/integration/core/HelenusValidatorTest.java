package net.helenus.test.integration.core;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusValidator;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.annotation.Constraints;
import net.helenus.mapping.annotation.PartitionKey;
import net.helenus.mapping.annotation.Table;
import net.helenus.support.HelenusException;
import net.helenus.support.HelenusMappingException;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.Before;
import org.junit.Test;

public class HelenusValidatorTest extends AbstractEmbeddedCassandraTest {

  @Table
  interface ModelForValidation {

    @Constraints.Email
    @PartitionKey
    String id();
  }

  HelenusEntity entity;

  HelenusProperty prop;

  @Before
  public void begin() {
    Helenus.init(getSession()).singleton();

    entity = Helenus.entity(ModelForValidation.class);

    prop = entity.getProperty("id");
  }

  @Test(expected = HelenusMappingException.class)
  public void testWrongType() {
    HelenusValidator.INSTANCE.validate(prop, Integer.valueOf(123));
  }

  @Test(expected = HelenusException.class)
  public void testWrongValue() {
    HelenusValidator.INSTANCE.validate(prop, "123");
  }

  public void testOk() {
    HelenusValidator.INSTANCE.validate(prop, "a@b.c");
  }
}
