package dev.morphia.annotations;

import dev.morphia.internal.CollationBuilder;
import dev.morphia.internal.FieldBuilder;
import dev.morphia.internal.IndexBuilder;
import dev.morphia.internal.IndexOptionsBuilder;
import dev.morphia.internal.IndexedBuilder;
import dev.morphia.internal.TextBuilder;
import dev.morphia.internal.ValidationBuilder;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static org.junit.Assert.assertNotNull;

public class AnnotationBuilderTest {
    @Test
    public void builders() throws NoSuchMethodException {
        compareFields(Index.class, IndexBuilder.class);
        compareFields(IndexOptions.class, IndexOptionsBuilder.class);
        compareFields(Indexed.class, IndexedBuilder.class);
        compareFields(Field.class, FieldBuilder.class);
        compareFields(Collation.class, CollationBuilder.class);
        compareFields(Text.class, TextBuilder.class);
        compareFields(Validation.class, ValidationBuilder.class);
    }

    private <T extends Annotation> void compareFields(Class<T> annotationType, Class<? extends AnnotationBuilder<T>> builder)
        throws NoSuchMethodException {

        for (Method method : annotationType.getDeclaredMethods()) {
            Method getter = builder.getDeclaredMethod(method.getName(), method.getReturnType());
            assertNotNull(String.format("Looking for %s.%s(%s) on ", builder.getSimpleName(), method.getName(), method.getReturnType()
                                                                                                                      .getSimpleName()),
                          getter);
        }
    }

}
