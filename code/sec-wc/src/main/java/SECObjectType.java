/**
 * SECObjectType - Type of SECObject expressed as an enum
 */


public enum SECObjectType {
    CONTACT,    // SECContact and contacts table
    DOCUMENT,   // SECDocument and documents table
    ENTITY,     // SECEntity and entities table
    FILING,     // SECFiling and filings table
    FORM10K,    // SEC form 10-k
    FORM4,      // SEC form 4
    FORM8K,     // SEC form 8-k
    HEADER,     // SEC filing header SGML
    OWNER_REL   // SECOwnerRel and owner_rels table
}

