package net.helenus.core.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.helenus.core.ConflictingUnitOfWorkException;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Retry {

	Class<? extends Exception>[] on() default ConflictingUnitOfWorkException.class;

	int times() default 3;
}
