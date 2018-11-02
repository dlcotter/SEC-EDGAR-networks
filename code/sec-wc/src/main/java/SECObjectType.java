/**
 * SECObjectType - Type of SECObject expressed as an enum
 */

public class SECObjectType {
    static int to_int(SECObjectEnum e) {
	int rc = 0;
	switch (e) {
	case NONE:      rc = 0; break;
	case CONTACT:   rc = 1; break;
	case DOCUMENT:  rc = 2; break;
	case ENTITY:    rc = 3; break;
	case FILING:    rc = 4; break;
	case FORM10K:   rc = 5; break;
	case FORM4:     rc = 6; break;
	case FORM8K:    rc = 7; break;
	case HEADER:    rc = 8; break;
	case OWNER_REL: rc = 9; break;
	default:        rc = 0; break;
	}
	return rc;
    }
}
	    
