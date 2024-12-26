package org.pritam.utils;

public class Pair<T, T1> {
    private T primary;
    private T1 secondary;

    public Pair(T primary, T1 secondary) {
        this.primary = primary;
        this.secondary = secondary;
    }

    public T getPrimary() {
        return primary;
    }

    public T1 getSecondary() {
        return secondary;
    }
}
