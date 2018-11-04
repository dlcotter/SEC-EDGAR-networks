/**
 * SECObjectFiling - Filing metadata
 *
 */

import java.io.StringBufferInputStream;
import java.text.DateFormat;
import java.text.DateFormat.Field;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

public class SECObjectFiling extends SECObject {
    private static final XMLInputFactory xmlParserFactory = XMLInputFactory.newInstance();

    static final String[] filingStrings = { "ACCESSION NUMBER:",
					    "CONFORMED SUBMISSION TYPE:",
					    "PUBLIC DOCUMENT COUNT:",
					    "FILED AS OF DATE:",
					    "DATE AS OF CHANGE:" };
    static final String [] filingKeys = { "accessionNumber",
				       "submissionType",
				       "documentCount",
				       "filingDate",
				       "changeDate" };

    private static final String headerSTag   = new String( "<SEC-HEADER>" );
    private static final String headerETag   = new String( "</SEC-HEADER>" );
    private static final String documentSTag = new String( "<DOCUMENT>" );
    private static final String documentETag = new String( "</DOCUMENT>" );
    private static final String filenameSTag = new String( "<FILENAME>" );
    private static final String textSTag     = new String( "<TEXT>" );
    private static final String textETag     = new String( "</TEXT>" );
    private static final String xmlSTag      = new String( "<XML>" );
    private static final String xmlETag      = new String( "</XML>" );

    static final Date badDate = new Date(0,0,1);
	
    private String accessionNumber;
    private String submissionType;
    private int    documentCount;
    private Date   filingDate;
    private Date   changeDate;
    private String fileContent;

    public SECObjectFiling( String accNum,
			    String subType,
			    int    docCount,
			    Date   filngDate,
			    Date   chngDate ) {
	objectType      = SECObjectType.FILING;
	accessionNumber = accNum;
	submissionType  = subType;
	documentCount   = docCount;
	filingDate      = filngDate;
	changeDate      = chngDate;
    }

    public SECObjectFiling( String fileContent ) {
	this.fileContent = fileContent;
    }
	
    private HashMap<String,String> getHeaderHashMap( String content, int startLoc, int endLoc ) {
	HashMap<String,String> filingValues = new HashMap<String,String>();

	for ( int i = 0; i < filingStrings.length; i++ ) {
	    // System.out.println("getHeaderHashMap: looking for: "+filingStrings[i] );
	    int loc1 = content.indexOf( filingStrings[i], startLoc);
	    if ( loc1 >= 0 ) {
		// System.out.println("getHeaderHashMap: found it at: "+Integer.toString(loc1));
		loc1 += filingStrings[i].length();
		char c = content.charAt(loc1);
		while (Character.isSpace(c)) {
		    loc1++;
		    c = content.charAt(loc1);
		}
		int loc2 = loc1;
		while (!Character.isSpace(c) && c != '\n') {
		    loc2++;
		    c = content.charAt(loc2);
		}
		if ( loc2-loc1 > 0 ) {
		    String value = content.substring(loc1,loc2);
		    filingValues.put( filingKeys[i], 
				      content.substring(loc1,loc2));
		    // System.out.println( "getHeaderHashMap: value="+value);
		}
	    }
	}
	return filingValues;
    }
	
    /**
     * Return a String representing the object as comma separated value record(s).
     */
    @Override
    public String toCSV() {
	SimpleDateFormat sdf0 = new SimpleDateFormat("YYYY-MM-dd");
	SimpleDateFormat sdf1 = new SimpleDateFormat("YYYY-MM-dd");
	String date0 = null;
	String date1 = null;
	StringBuffer csvline = new StringBuffer(100);

	try {
	    date0 = sdf0.format(filingDate);
	} catch (Exception e) {
	    date0 = sdf0.format(badDate);
	}
	try {
	    date1 = sdf1.format(changeDate); 
	} catch (Exception e) {
	    date1 = date0;
	}

	csvline.delete(0,csvline.capacity());
	csvline.append(accessionNumber);
	csvline.append('|');
	csvline.append(submissionType);
	csvline.append('|');
	csvline.append(documentCount);
	csvline.append('|');
	csvline.append(date0);
	csvline.append('|');
	csvline.append(date1);
	return csvline.toString();
    }

    private ArrayList<SECObject>  parseXML( String content, int startLoc, int endLoc )
	throws XMLStreamException {
	//	System.out.println( "parseXML: starting" );
	ArrayList<SECObject> objects = new ArrayList<SECObject>();
	SimpleDateFormat sdf     = new SimpleDateFormat("YYYYMMdd");
	String accession_number= "";
	String submission_type = "";
	StringBufferInputStream xmlInput = 
	    new StringBufferInputStream( content.substring(startLoc, endLoc ));
	XMLEventReader xmlReader = xmlParserFactory.createXMLEventReader(xmlInput);
	int state = 0;
	//                                     State
	String documentType        = null;  //    1
	String periodOfReport      = null;  //    2
	String issuerCik           = null;  //   31
	String issuerName          = null;  //   32
	String issuerTradingSymbol = null;  //   33
	String rptOwnerCik         = null;  //  411
	String rptOwnerName        = null;  //  412
	String rptOwnerStreet1     = null;  //  421
	String rptOwnerStreet2     = null;  //  422
	String rptOwnerCity        = null;  //  423
	String rptOwnerState       = null;  //  424
	String rptOwnerZipCode     = null;  //  425
	String isDirector          = null;  //  431
	String isOfficer           = null;  //  432
	String isTenPercentOwner   = null;  //  433
	String isOther             = null;  //  434
	String officerTitle        = null;  //  435
	String otherText           = null;  //  436

	// System.out.println( "parseXML: starting to read elements" );
	
	while (xmlReader.hasNext()) {
	    XMLEvent event = xmlReader.nextEvent();
	    switch (event.getEventType()) {
	    case XMLStreamConstants.START_ELEMENT: {
		StartElement startElement = event.asStartElement();
		String qName = startElement.getName().getLocalPart();

		// System.out.println("XMLParser: found "+qName+" element");
		if ( qName.equalsIgnoreCase("documentType")) {
		    state = 1;
		} else if ( qName.equalsIgnoreCase("periodOfReport")) {
		    state = 2;
		} else if ( qName.equalsIgnoreCase("issuer")) {
		    state = 3;
		} else if ( qName.equalsIgnoreCase("issuerCik")) {
		    state = 31;
		} else if ( qName.equalsIgnoreCase("issuerName")) {
		    state = 32;
		} else if ( qName.equalsIgnoreCase("issuerTradingSymbol")) {
		    state = 33;
		} else if ( qName.equalsIgnoreCase("reportingOwner")) {
		    state = 4;
		} else if ( qName.equalsIgnoreCase("reportingOwnerId")) {
		    state = 41;
		} else if ( qName.equalsIgnoreCase("rptOwnerCik")) {
		    state = 411;
		} else if ( qName.equalsIgnoreCase("rptOwnerName")) {
		    state = 412;
		} else if ( qName.equalsIgnoreCase("reportingOwnerAddress")) {
		    state = 42;
		} else if ( qName.equalsIgnoreCase("rptOwnerStreet1")) {
		    state = 421;
		} else if ( qName.equalsIgnoreCase("rptOwnerStreet2")) {
		    state = 422;
		} else if ( qName.equalsIgnoreCase("rptOwnerCity")) {
		    state = 423;
		} else if ( qName.equalsIgnoreCase("rptOwnerState")) {
		    state = 424;
		} else if ( qName.equalsIgnoreCase("rptOwnerZipCode")) {
		    state = 425;
		} else if ( qName.equalsIgnoreCase("reportingOwnerRelationship")) {
		    state = 43;
		} else if ( qName.equalsIgnoreCase("isDirector")) {
		    state = 431;
		} else if ( qName.equalsIgnoreCase("isOfficer")) {
		    state = 432;
		} else if ( qName.equalsIgnoreCase("isTenPercentOwner")) {
		    state = 433;
		} else if ( qName.equalsIgnoreCase("isOther")) {
		    state = 434;
		} else if ( qName.equalsIgnoreCase("officerTitle")) {
		    state = 435;
		} else if ( qName.equalsIgnoreCase("otherText")) {
		    state = 436;
		}
		break;
	    }
	    case XMLStreamConstants.CHARACTERS: {
		String data = event.asCharacters().getData();
		switch (state) {
		case   2: periodOfReport      = data; break;
		case  31: issuerCik           = data; break;
		case  32: issuerName          = data; break;
		case  33: issuerTradingSymbol = data; break;
		case 411: rptOwnerCik         = data; break;
		case 412: rptOwnerName        = data; break;
		case 421: rptOwnerStreet1     = data; break;
		case 422: rptOwnerStreet2     = data; break;
		case 423: rptOwnerCity        = data; break;
		case 424: rptOwnerState       = data; break;
		case 425: rptOwnerZipCode     = data; break;
		case 431: isDirector          = data; break;
		case 432: isOfficer           = data; break;
		case 433: isTenPercentOwner   = data; break;
		case 434: isOther             = data; break;
		case 435: officerTitle        = data; break;
		case 436: otherText           = data; break;
		default: break;
		}
		break;
	    }
	    case XMLStreamConstants.END_ELEMENT: {
		EndElement endElement = event.asEndElement();
		String qName = endElement.getName().getLocalPart();
		if ( qName.equalsIgnoreCase("documentType")) {
		    state = 0;
		} else if ( qName.equalsIgnoreCase("periodOfReport")) {
		    state = 1;
		} else if ( qName.equalsIgnoreCase("issuer")) {
		    // entities
		    // filings-entities
		    
		    state = 1;
		} else if ( qName.equalsIgnoreCase("issuerCik")) {
		    state = 3;
		} else if ( qName.equalsIgnoreCase("issuerName")) {
		    state = 3;
		} else if ( qName.equalsIgnoreCase("issuerTradingSymbol")) {
		    state = 3;
		} else if ( qName.equalsIgnoreCase("reportingOwner")) {
		    // entities
		    SECObjectEntity entity = 
			new SECObjectEntity( rptOwnerCik,  // cik
					     null,         // tradingSymbol
					     rptOwnerName, // entityName
					     "owner",      // entityType
					     null,         // irsNumber
					     null,         // sic
					     null,         // sicNumber
					     null,         // stateOfInc
					     null );       // fiscalYearEnd
		    objects.add( entity );
		    // filings-entities
		    SECObjectFilingEntity filingEntity =
			new SECObjectFilingEntity( accessionNumber,
						   rptOwnerCik );
		    objects.add(filingEntity);
		    rptOwnerCik = null;
		    rptOwnerName = null;
		    // owner_rels
		    state = 1;
		} else if ( qName.equalsIgnoreCase("reportingOwnerId")) {
		    state = 4;
		} else if ( qName.equalsIgnoreCase("rptOwnerCik")) {
		    state = 41;
		} else if ( qName.equalsIgnoreCase("rptOwnerName")) {
		    state = 41;
		} else if ( qName.equalsIgnoreCase("reportingOwnerAddress")) {
		    state = 4;
		} else if ( qName.equalsIgnoreCase("rptOwnerStreet1")) {
		    state = 42;
		} else if ( qName.equalsIgnoreCase("rptOwnerStreet2")) {
		    state = 42;
		} else if ( qName.equalsIgnoreCase("rptOwnerCity")) {
		    state = 42;
		} else if ( qName.equalsIgnoreCase("rptOwnerState")) {
		    state = 42;
		} else if ( qName.equalsIgnoreCase("rptOwnerZipCode")) {
		    state = 42;
		} else if ( qName.equalsIgnoreCase("reportingOwnerRelationship")) {
		    state = 4;
		} else if ( qName.equalsIgnoreCase("isDirector")) {
		    state = 43;
		} else if ( qName.equalsIgnoreCase("isOfficer")) {
		    state = 43;
		} else if ( qName.equalsIgnoreCase("isTenPercentOwner")) {
		    state = 43;
		} else if ( qName.equalsIgnoreCase("isOther")) {
		    state = 43;
		} else if ( qName.equalsIgnoreCase("officerTitle")) {
		    state = 43;
		} else if ( qName.equalsIgnoreCase("otherText")) {
		    state = 43;
		}
		break;
	    }
	    }
	}
	return objects;
    }

    private ArrayList<SECObject> parseHeader(String content, int headerStart, int headerEnd ) {
	ArrayList<SECObject> objects = new ArrayList<SECObject>();
	SimpleDateFormat sdf     = new SimpleDateFormat("YYYYMMdd");
	String accession_number= "";
	String submission_type = "";
	int    document_count  = 0;
	Date   filing_date = null;
	Date   change_date = null;
	HashMap headerData = getHeaderHashMap( content, headerStart+12, headerEnd);
	for ( int i=0; i < filingKeys.length; i++ ) {
	    String value = (java.lang.String) headerData.get( filingKeys[i]);
	    if ( value != null ) {
		switch (i) {
		case 0: accession_number = value; break;
		case 1: submission_type  = value; break;
		case 2: document_count  = Integer.parseInt( value ); break;
		case 3: {
		    String yr = value.substring(0,4);
		    String mn = value.substring(4,6);
		    String dy = value.substring(6,8);
		    int year  = Integer.parseInt(yr);
		    int month = Integer.parseInt(mn);
		    int day   = Integer.parseInt(dy);
		    filing_date       = new Date(year-1900, month-1, day );
		    // System.out.println( "filing-date:  "+Integer.toString(year)
		    //		    +"-"+Integer.toString(month)+"-"+Integer.toString(day) );
		} break;
		case 4: {
		    String yr = value.substring(0,4);
		    String mn = value.substring(4,6);
		    String dy = value.substring(6,8);
		    int year  = Integer.parseInt(yr);
		    int month = Integer.parseInt(mn);
		    int day   = Integer.parseInt(dy);
		    change_date       = new Date(year-1900, month-1, day );
		    // System.out.println( "change-date:  "+yr+"-"+mn+"-"+dy );
		} break;
		}
		// System.out.println("parse: key: "+filingKeys[i]+"  value: "+value);
	    }
	}
	accessionNumber = accession_number;
	SECObjectFiling filing = new SECObjectFiling( accession_number,
						      submission_type,
						      document_count,
						      filing_date,
						      change_date );
	objects.add(filing);
	return objects;
    }

    /**
     * Return an Array of SECObjects when parsing a component
     */
    public ArrayList<SECObject> parse( String content, int startLoc, int endLoc ) {
	// // System.out.println( "parse:  content="+content);

	ArrayList<SECObject> objects = new ArrayList<SECObject>(10);

	int headerStart = content.indexOf( headerSTag, startLoc );
	int headerEnd   = content.indexOf( headerETag, startLoc + headerSTag.length());
	int docStart    = 0;
	int docEnd      = 0;
	
	// System.out.println( "parse:  headerStart="+headerStart );
	// System.out.println( "parse:  headerEnd=  "+headerEnd );
	if ( headerStart >= startLoc && headerEnd >= startLoc && headerEnd <= endLoc ) {
	    ArrayList<SECObject> newObjs  = parseHeader( content, headerStart, headerEnd );
	    if ( newObjs != null ) {
		objects.addAll( newObjs );
	    }
	}
	// System.out.println( "parse: have "+objects.size()+" objects" );
	if ( headerEnd >= 0 ) {
	    String nextStr = content.substring(headerEnd+headerETag.length(),headerEnd+headerETag.length());
	    // System.out.println( "parse: Starting doc search @ "+(headerEnd+headerETag.length())+"  "+nextStr);

	    docStart = content.indexOf( documentSTag, headerEnd+headerETag.length() );
	}
	if ( docStart > 0 ) {
	    // System.out.println( "parse: Starting docEnd search at "+(docStart+documentSTag.length()));
	    docEnd   = content.indexOf( documentETag, docStart+documentSTag.length() );
	}

	// System.out.println( "parse:  docStart="+docStart );
	// System.out.println( "parse:  docEnd=  "+docEnd );
	while ( docStart > 0 && docStart >= startLoc && docEnd <= endLoc ) {
	    int xmlStart = content.indexOf( xmlSTag, docStart );
	    int xmlEnd   = content.indexOf( xmlETag, docStart+xmlSTag.length());

	    // System.out.println( "parse:  xmlStart="+xmlStart );
	    // System.out.println( "parse:    xmlEnd="+xmlEnd );
	    if ( xmlStart > startLoc &&
		 xmlStart > docStart &&
		 xmlEnd   > docStart &&
		 xmlEnd <= endLoc ) {
		try {
		    // System.out.println( "parseXML: Beginning parseXML" );
		    ArrayList<SECObject> newObjs  = parseXML( content, (xmlStart+xmlSTag.length()+1), xmlEnd );
		    // System.out.println( "parseXML: Return from parseXML");
	            // System.out.println( "parse: have "+objects.size()+" objects" );
		    if ( newObjs != null ) {
			objects.addAll( newObjs );
		    }
		} catch (XMLStreamException e) {
		    // System.out.println( "parseXML failed: "+e.getMessage() );
		    return null;
		}
		docStart = docEnd + documentSTag.length();
		docStart = content.indexOf( documentSTag, docStart );
		docEnd   = content.indexOf( documentETag, docStart+documentSTag.length());
		// System.out.println( "parse:  docStart="+docStart );
		// System.out.println( "parse:  docEnd=  "+docEnd );
	    } else {
		break;
	    }
	}
	// System.out.println( "parse: have "+objects.size()+" objects");
	if ( objects.size() > 0 ) {
	    return objects;
	} else {
	    return null;
	}
    }
}


