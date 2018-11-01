/**
 * SECObjectFiling - Filing metadata
 *
 */

import java.util.Date;
import java.util.HashMap;
import java.text.DateFormat;
import java.text.DateFormat.Field;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

public class SECObjectFiling extends SECObject {
    SECObjectType objectType;

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
	
    
    private String accessionNumber;
    private String submissionType;
    private int    documentCount;
    private Date   filingDate;
    private Date   changeDate;


    public SECObjectFiling( String accNum,
			    String subType,
			    int    docCount,
			    Date   filngDate,
			    Date   chngDate ) {
	accessionNumber = accNum;
	submissionType  = subType;
	documentCount   = docCount;
	filingDate      = filngDate;
	changeDate      = chngDate;
    }
	
    public SECObjectType getType() {
	return SECObjectType.FILING;
    }


    private static HashMap<String,String> getHeaderHashMap( String content, int startLoc, int endLoc ) {
	HashMap<String,String> filingValues = new HashMap<String,String>();

	for ( int i = 0; i < filingStrings.length; i++ ) {
	    System.out.println("getHeaderHashMap: looking for: "+filingStrings[i] );
	    int loc1 = content.indexOf( filingStrings[i], startLoc);
	    if ( loc1 >= 0 ) {
		System.out.println("getHeaderHashMap: found it at: "+Integer.toString(loc1));
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
		    System.out.println( "getHeaderHashMap: value="+value);
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
	String date0 = sdf0.format(filingDate);
	String date1 = sdf1.format(changeDate);
	String csvLine =    accessionNumber+
                        ","+submissionType+
	                ","+documentCount+
                        ","+date0+
                        ","+date1;
	System.out.println( "toCSV: "+csvLine );
	return csvLine;
    }

    /**
     * Return an Array of SECObjects when parsing a component
     */
    public static SECObject[] parse( String content, int startLoc, int endLoc ) {
	SECObject[] objects = new SECObject[1];
	SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
	
	System.out.println( "parse:  content="+content);

	int headerStart = content.indexOf( "<SEC-HEADER>" );
	int headerEnd   = content.indexOf( "</SEC-HEADER>" );

	System.out.println( "parse:  headerStart="+headerStart );
	System.out.println( "parse:  headerEnd=  "+headerEnd );
	if ( headerStart >= startLoc && headerEnd <= endLoc ) {
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
			System.out.println( "filing-date:  "+Integer.toString(year)
					    +"-"+Integer.toString(month)+"-"+Integer.toString(day) );
		    } break;
		    case 4: {
			String yr = value.substring(0,4);
			String mn = value.substring(4,6);
			String dy = value.substring(6,8);
			int year  = Integer.parseInt(yr);
			int month = Integer.parseInt(mn);
			int day   = Integer.parseInt(dy);
			change_date       = new Date(year-1900, month-1, day );
			System.out.println( "change-date:  "+yr+"-"+mn+"-"+dy );
		    } break;
		    }
		    System.out.println("parse: key: "+filingKeys[i]+"  value: "+value);
		} else {
		    System.out.println("parse: key: "+filingKeys[i]+"  value: null");
		}
	    }
	    SECObjectFiling filing = new SECObjectFiling( accession_number,
							  submission_type,
							  document_count,
							  filing_date,
							  change_date );
	    objects[0] = filing;
	    return objects;
	} else {
	    return null;
	}
    }
}


