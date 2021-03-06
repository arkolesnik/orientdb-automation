import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.io.File;

public class CreateDatabaseForLoadFixture {

    ODatabaseDocumentTx database;
    private static final String ARG_DIR = "db.dir";
    private static final String NO_ARG_DIR = "java.io.tmpdir";
    private static final String DB_NAME = "Testindexes";
    static String PATH;
    static final String USER = "admin";
    static final String PASSWORD = "admin";
    static final String CLASS_NAME = "NewData";
    static final String ID_NAME = "id";
    static final String INTEGER_PROPERTY_NAME = "integerProperty";
    static final String LIST_PROPERTY_NAME = "listProperty";
    static final String SET_PROPERTY_NAME = "setProperty";
    static final String MAP_PROPERTY_NAME = "mapProperty";

    static final String ID_INDEX = CLASS_NAME + "." + ID_NAME;
    static final String INTEGER_PROPERTY_INDEX = CLASS_NAME + "." + INTEGER_PROPERTY_NAME;
    static final String LIST_PROPERTY_INDEX = CLASS_NAME + "." + LIST_PROPERTY_NAME;
    static final String SET_PROPERTY_INDEX = CLASS_NAME + "." + SET_PROPERTY_NAME;
    static final String MAP_PROPERTY_INDEX = CLASS_NAME + "." + MAP_PROPERTY_NAME;

    static final String COMPOSITE_INDEX_1 = CLASS_NAME + ".composite1";
    static final String COMPOSITE_INDEX_2 = CLASS_NAME + ".composite2";

    static final int RECORDS_NUMBER = 100000;
    static final int THREADS = 8;

    @BeforeTest
    public void createDatabase() {
        String url = System.getProperty(ARG_DIR);
        if (url == null) {
            url = System.getProperty(NO_ARG_DIR);
        }

        PATH = new File(url + File.separator + DB_NAME).getPath();

        if (PATH.contains("remote")) {
            database = new ODatabaseDocumentTx(PATH).open(USER, PASSWORD);
        } else {
            database = new ODatabaseDocumentTx("plocal:" + PATH).create();
        }

        OClass data = database.getMetadata().getSchema().createClass(CLASS_NAME);
        data.createProperty(ID_NAME, OType.INTEGER);
        data.createProperty(INTEGER_PROPERTY_NAME, OType.INTEGER);
        data.createProperty(LIST_PROPERTY_NAME, OType.EMBEDDEDLIST, OType.INTEGER);
        data.createProperty(SET_PROPERTY_NAME, OType.EMBEDDEDSET, OType.INTEGER);
        data.createProperty(MAP_PROPERTY_NAME, OType.EMBEDDEDMAP, OType.INTEGER);
    }

    @AfterTest
    public void dropDatabase() {
        if (!database.getStorage().isRemote()) {
            database.drop();
        }
    }
}
