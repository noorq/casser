/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package net.helenus.core.aspect;

import java.lang.reflect.Method;
import java.util.Arrays;
import net.helenus.core.annotation.Retry;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.Assert;

@Aspect
public class RetryAspect {

  private static final Logger log = LoggerFactory.getLogger(RetryAspect.class);

  @Around("@annotation(net.helenus.core.annotations.Retry)")
  public Object retry(ProceedingJoinPoint pjp) throws Throwable {
    Retry retryAnnotation = getRetryAnnotation(pjp);
    return (retryAnnotation != null) ? proceed(pjp, retryAnnotation) : proceed(pjp);
  }

  private Object proceed(ProceedingJoinPoint pjp) throws Throwable {
    return pjp.proceed();
  }

  private Object proceed(ProceedingJoinPoint pjp, Retry retryAnnotation) throws Throwable {
    int times = retryAnnotation.times();
    Class<? extends Throwable>[] retryOn = retryAnnotation.on();
    Assert.isTrue(times > 0, "@Retry{times} should be greater than 0!");
    Assert.isTrue(retryOn.length > 0, "@Retry{on} should have at least one Throwable!");
    log.info("Proceed with {} retries on {}", times, Arrays.toString(retryOn));
    return tryProceeding(pjp, times, retryOn);
  }

  private Object tryProceeding(
      ProceedingJoinPoint pjp, int times, Class<? extends Throwable>[] retryOn) throws Throwable {
    try {
      return proceed(pjp);
    } catch (Throwable throwable) {
      if (isRetryThrowable(throwable, retryOn) && times-- > 0) {
        log.info("Conflict detected, {} remaining retries on {}", times, Arrays.toString(retryOn));
        return tryProceeding(pjp, times, retryOn);
      }
      throw throwable;
    }
  }

  private boolean isRetryThrowable(Throwable throwable, Class<? extends Throwable>[] retryOn) {
    Throwable[] causes = ExceptionUtils.getThrowables(throwable);
    for (Throwable cause : causes) {
      for (Class<? extends Throwable> retryThrowable : retryOn) {
        if (retryThrowable.isAssignableFrom(cause.getClass())) {
          return true;
        }
      }
    }
    return false;
  }

  private Retry getRetryAnnotation(ProceedingJoinPoint pjp) throws NoSuchMethodException {
    MethodSignature signature = (MethodSignature) pjp.getSignature();
    Method method = signature.getMethod();
    Retry retryAnnotation = AnnotationUtils.findAnnotation(method, Retry.class);

    if (retryAnnotation != null) {
      return retryAnnotation;
    }

    Class<?>[] argClasses = new Class[pjp.getArgs().length];
    for (int i = 0; i < pjp.getArgs().length; i++) {
      argClasses[i] = pjp.getArgs()[i].getClass();
    }
    method = pjp.getTarget().getClass().getMethod(pjp.getSignature().getName(), argClasses);
    return AnnotationUtils.findAnnotation(method, Retry.class);
  }
}
