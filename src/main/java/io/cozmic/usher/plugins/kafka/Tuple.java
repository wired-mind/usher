package io.cozmic.usher.plugins.kafka;

import com.google.common.base.Preconditions;

/**
 * A generic 2-tuple.
 * <p>
 * Created by Craig Earley on 12/23/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class Tuple<T, U> {
    public final T _1;
    public final U _2;

    public Tuple(T arg1, U arg2) {
        Preconditions.checkNotNull(arg1, "arg1 cannot be null");
        Preconditions.checkNotNull(arg2, "arg2 cannot be null");
        this._1 = arg1;
        this._2 = arg2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple<?, ?> tuple = (Tuple<?, ?>) o;

        if (!_1.equals(tuple._1)) return false;
        return _2.equals(tuple._2);
    }

    @Override
    public int hashCode() {
        int result = _1.hashCode();
        result = 31 * result + _2.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", _1, _2);
    }
}
