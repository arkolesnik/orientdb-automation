package utils;
import java.util.Random;

public enum Operations {

    CREATE,
    UPDATE,
    DELETE;

    public static Operations getRandomFrom(Operations... params) {
        return params[new Random().nextInt(params.length)];
    }
}
