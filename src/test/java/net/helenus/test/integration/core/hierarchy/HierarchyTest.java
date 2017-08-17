package net.helenus.test.integration.core.hierarchy;

import static net.helenus.core.Query.eq;

import java.util.Optional;
import java.util.Random;
import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HierarchyTest extends AbstractEmbeddedCassandraTest {

  static Cat cat;

  static Pig pig;

  static HelenusSession session;

  static Random rnd = new Random();

  @BeforeClass
  public static void beforeTest() {
    session =
        Helenus.init(getSession()).showCql().add(Cat.class).add(Pig.class).autoCreateDrop().get();
    cat = Helenus.dsl(Cat.class);
    pig = Helenus.dsl(Pig.class);
  }

  @Test
  public void testPrint() {
    System.out.println(cat);
  }

  @Test
  public void testCounter() {

    session
        .insert()
        .value(cat::id, rnd.nextInt())
        .value(cat::nickname, "garfield")
        .value(cat::eatable, false)
        .sync();
    session
        .insert()
        .value(pig::id, rnd.nextInt())
        .value(pig::nickname, "porky")
        .value(pig::eatable, true)
        .sync();

    Optional<Cat> animal =
        session.select(Cat.class).where(cat::nickname, eq("garfield")).sync().findFirst();
    Assert.assertTrue(animal.isPresent());
    Assert.assertFalse(animal.get().eatable());
  }

  @Test
  public void testDefaultMethod() {
    session
        .insert()
        .value(cat::id, rnd.nextInt())
        .value(cat::nickname, "garfield")
        .value(cat::eatable, false)
        .sync();
    Optional<Cat> animal =
        session.select(Cat.class).where(cat::nickname, eq("garfield")).sync().findFirst();
    Assert.assertTrue(animal.isPresent());

    Cat cat = animal.get();
    Animal itsme = cat.me();
    Assert.assertEquals(cat, itsme);
  }
}
