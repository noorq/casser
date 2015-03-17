package casser.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.datastax.driver.core.DataType;

@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Qualify {

	DataType.Name type();

	DataType.Name[] typeArguments() default {};

}
