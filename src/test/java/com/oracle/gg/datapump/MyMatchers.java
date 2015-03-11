package com.oracle.gg.datapump;

import java.util.Collections;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class MyMatchers {
    public static <T> Matcher<List<T>> list(final Class<T> type, final int num_elements) {
        return new BaseMatcher<List<T>>(){
            @Override
            public boolean matches(Object o) {
                List<T> actualList = Collections.emptyList();
                try {
                    actualList = (List<T>) o;
                }catch (ClassCastException e) {
                    return false;
                }
                
                return actualList.size() == num_elements;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("should contain " + num_elements + " elements of ")
                		.appendText(type.getName().toString());
            }
        };
    }
}
