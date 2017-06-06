package org.inanme;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;

public class Functions {
    @SuppressWarnings("unchecked")
    public static <T> Collector<T, ArrayList<T>, T[]> toArray(Class<T> clazz) {
        return Collector.of(ArrayList::new, List::add, (left, right) -> {
            left.addAll(right);
            return left;
        }, list -> list.toArray((T[]) Array.newInstance(clazz, list.size())));
    }

    public static void mysleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
