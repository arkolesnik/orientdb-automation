package utils;

import java.util.Random;

public enum Fields {

    INTEGER_PROPERTY,
    LIST_PROPERTY,
    SET_PROPERTY,
    MAP_PROPERTY;

    public static Fields getRandom() {
        return values()[new Random().nextInt(values().length)];
    }


}
