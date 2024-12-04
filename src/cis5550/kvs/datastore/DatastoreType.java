package cis5550.kvs.datastore;

public enum DatastoreType {
    PERSISTENT("pt-"),
    APPEND_ONLY("at-"),
    IN_MEMORY(""),
    ;

    private final String prefix;

    DatastoreType(String prefix) {
        this.prefix = prefix;
    }

    public String prefix() {
        return prefix;
    }

    public static DatastoreType fromName(String aName) {
        for (DatastoreType type : DatastoreType.values()) {
            if (aName.startsWith(type.prefix())) {
                return type;
            }
        }
        return IN_MEMORY;
    }
}


