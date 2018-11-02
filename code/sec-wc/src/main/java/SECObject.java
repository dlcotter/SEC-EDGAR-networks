/**
 * SECObject - Documents and forms extracted from SEC filings
 *
 */

public abstract class SECObject {
    SECObjectEnum objectType;


    public SECObjectEnum getType() {
	return objectType;
    }

    /**
     * Return a String representing the object as comma separated value record(s).
     */
    public abstract String toCSV();

    /**
     * Return an Array of SECObjects when parsing a component
     */
    public static SECObject[] parse(String content, int start, int end){
	return null;
    }
}

