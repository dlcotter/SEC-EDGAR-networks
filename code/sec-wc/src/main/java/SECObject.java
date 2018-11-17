/**
 * SECObject - Documents and forms extracted from SEC filings
 *
 */

import java.util.ArrayList;

public abstract class SECObject {
    SECObjectType objectType;


    public SECObjectType getType() {
	return objectType;
    }

    /**
     * Return a String representing the object as comma separated value record(s).
     */
    public abstract String toCSV();

    /**
     * Return an Array of SECObjects when parsing a component
     */
    public ArrayList<SECObject> parse(String content, int start, int end){
	return null;
    }
}

