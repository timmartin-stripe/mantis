package io.reactivex.mantis.network.push;

public class AsyncConnectionChange<T> {
    public enum Type {
        ADD, REMOVE
    }

    private final Type type;
    private final AsyncConnection<T> connection;

    public AsyncConnectionChange(Type type, AsyncConnection<T> connection) {
        this.type = type;
        this.connection = connection;
    }

    public Type getType() {
        return type;
    }

    public AsyncConnection<T> getConnection() {
        return connection;
    }
}
