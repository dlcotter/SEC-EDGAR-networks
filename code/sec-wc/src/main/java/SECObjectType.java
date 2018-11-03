/**
 * SECObjectType - Type of SECObject expressed as a String
 */

public class SECObjectType {
    private int type;

    public SECObjectType( int type ) { this.type = type; }

    public static final SECObjectType NONE      = new SECObjectType(0);
    public static final SECObjectType CONTACT   = new SECObjectType(1);
    public static final SECObjectType DOCUMENT  = new SECObjectType(2);
    public static final SECObjectType ENTITY    = new SECObjectType(3);
    public static final SECObjectType FILING    = new SECObjectType(4);
    public static final SECObjectType FORM10K   = new SECObjectType(5);
    public static final SECObjectType FORM4     = new SECObjectType(6);
    public static final SECObjectType FORM8K    = new SECObjectType(7);
    public static final SECObjectType HEADER    = new SECObjectType(8);
    public static final SECObjectType OWNER_REL = new SECObjectType(9);

    private static final String NONE_S      = "none";
    private static final String CONTACT_S   = "contact";
    private static final String DOCUMENT_S  = "document";
    private static final String ENTITY_S    = "entity";
    private static final String FILING_S    = "filing";
    private static final String FORM10K_S   = "form10k";
    private static final String FORM4_S     = "form4";
    private static final String FORM8K_S    = "form8k";
    private static final String HEADER_S    = "header";
    private static final String OWNER_REL_S = "owner_rel";

    private static final String[] names = { NONE_S, CONTACT_S,DOCUMENT_S, ENTITY_S, FILING_S,
                                            FORM10K_S, FORM4_S,  FORM8K_S,   HEADER_S, OWNER_REL_S };
    private static final SECObjectType[] types = { NONE, CONTACT, DOCUMENT, ENTITY, FILING,
				 	           FORM10K, FORM4, FORM8K, HEADER, OWNER_REL };
    
    public String to_string() {
       if ( type >= 0 && type <= names.length ) {
          return names[type];
       } else {
          return null;
       }
    }

    public int to_int() {
	return type;
    }

    public static SECObjectType parse( String name ) {
	int lo = 1;
	int mid = names.length /2;
	int hi = names.length-1;

	// System.out.println("SECObjectType.parse0: name="+name+"  lo="+lo+"  mid="+mid+"  hi="+hi );
	int rc = name.compareTo( names[mid] );
	// System.out.println("SECObjectType.parse1: rc="+rc+" mid="+mid+"  mid_s="+names[mid]);
	while ( rc != 0 && lo <= hi) {
	    // System.out.println("SECObjectType.parse2: rc="+rc+"  lo="+lo+"  mid="+mid+"  hi="+hi+"  mid_s="+names[mid] );
	    if ( rc > 0 ) {
		lo = mid+1;
		mid = ( lo + hi ) / 2;
	    } else {
		hi = mid-1;
		mid = ( lo + hi ) / 2;
	    }
	    rc = name.compareTo( names[mid] );
	}
	// System.out.println("SECObjectType.parse3: rc="+rc+" mid="+mid+"  mid_s="+names[mid]);
	if ( rc == 0 ) {
	    return types[ mid ];
	} else {
	    return SECObjectType.NONE;
	}
    }
}
	    
