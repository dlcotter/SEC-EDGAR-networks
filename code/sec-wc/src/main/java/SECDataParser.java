import java.io.*;

class SECDataParser {

    private static int inputBufferSize = 65536;
    private static int maxWordSize     =  1024;
    private static int minWordSize     =     4;

    // states

    private static int st_START    = 0;
    private static int st_TAG      = 1;
    private static int st_SPACE    = 2;
    private static int st_WORD     = 3;
    private static int st_END      = 4;
    private static int st_BINARY   = 5;
    private static int st_DOCUMENT = 6;
    private static int st_SYMBOL   = 7;

    private InputStreamReader isr;
    private char[]            word;
    private String            content;
    private int               wordLoc;
    private Stemmer           stemmer;
    private int               state      = st_START;
    private int               nCharsRead = 0;

    public SECDataParser( InputStream inStream ) throws IOException {
	isr = new InputStreamReader( inStream );
	word = new char[maxWordSize];
	wordLoc = 0;
	stemmer = new Stemmer();
	state = st_START;
	nCharsRead = 0;
    }

    public SECDataParser( String fileContent ) throws IOException {
	content = fileContent;
    }

    public SECObject nextObject() throws IOException {
	String  newWord = new String();
	boolean haveWord = false;
	int     charin   = 0;
	char    inChar;
	while ( !haveWord && (charin = isr.read()) != -1 ) {
	    inChar = (char)charin;
	    // System.out.println( "nextWord: state = "+Long.toString(state)+"  char = "+Character.toString(inChar));
	    if ( state == st_START ) {
		if ( inChar == '<' ) {
		    state = st_TAG;
		    wordLoc = 0;
		    word[wordLoc] = inChar;
		    wordLoc++;
		} else if ( Character.isLetter( inChar)) {
		    state = st_WORD;
		    wordLoc = 0;
		    word[wordLoc] = Character.toLowerCase(inChar);
		    wordLoc++;
		} else if ( Character.isSpace( inChar)) {
		    state = st_SPACE;
		} else if ( inChar == '&' ) {
		    state = st_SYMBOL;
		}
	    } else if ( state == st_TAG ) {
		if ( inChar == '>' ) {
		    // System.out.println("TAG: "+String.valueOf(word,0,wordLoc));
		    if ( String.valueOf( word, 0, wordLoc ).compareTo("<DOCUMENT") == 0 ) {
			state = st_DOCUMENT;
			word[wordLoc] = inChar;
			wordLoc++;
			// System.out.println("Found DOCUMENT: " + String.valueOf(word,0,wordLoc));
		    } else {
			wordLoc = 0;
			state = st_START;
		    }
		} else {
		    if ( wordLoc < maxWordSize ) {
			word[wordLoc] = inChar;
			wordLoc++;
		    }
		}
	    } else if ( state == st_WORD ) {
		if ( Character.isLetter( inChar) // ||
		     // Character.isDigit( inChar)  ||
		     //	inChar == '-'              ||
		     // inChar == '_'
		     ) {
		    if ( wordLoc < maxWordSize ) {
			word[wordLoc] = Character.toLowerCase(inChar);
			wordLoc++;
		    }
		} else if ( inChar == '<' ) {
		    if ( wordLoc >= minWordSize ) {
			stemmer.add( word, wordLoc );
			stemmer.stem();
			newWord =  stemmer.toString();
			haveWord = true;
		    }
		    wordLoc = 0;
		    word[wordLoc] = inChar;
		    wordLoc++;
		    state = st_TAG;
		} else if ( Character.isSpace( inChar)) {
		    if ( wordLoc >= minWordSize  ) {
			stemmer.add( word, wordLoc );
			stemmer.stem();
			newWord =  stemmer.toString();
			haveWord = true;
			wordLoc = 0;
		    }
		    state = st_SPACE;
		} else {
		    if ( wordLoc >= minWordSize  ) {
			stemmer.add( word, wordLoc );
			stemmer.stem();
			newWord =  stemmer.toString();
			haveWord = true;
			wordLoc = 0;
		    }
		    state = st_START;
		}
	    } else if ( state == st_SYMBOL ) {
		if ( inChar == ';' ) {
		    wordLoc = 0;
		    state = st_START;
		}
	    } else if ( state == st_SPACE ) {
		if ( inChar == '<' ) {
		    wordLoc = 0;
		    word[wordLoc] = inChar;
		    wordLoc++;
		    state = st_TAG;
		} else if ( Character.isLetter( inChar)) {
		    wordLoc = 0;
		    word[wordLoc] = Character.toLowerCase(inChar);
		    wordLoc++;
		    state = st_WORD;
		} else if ( Character.isSpace( inChar)) {
		    state = st_SPACE;
		} else if ( inChar == '&' ) {
		    wordLoc = 0;
		    word[wordLoc] = inChar;
		    wordLoc++;
		    state = st_SYMBOL;
		} else {
		    state = st_START;
		}
	    } else if ( state == st_DOCUMENT ) {
		if ( inChar == '\n' ) {
		    String docTag = String.valueOf(word,0,wordLoc);
		    // System.out.println("DOC: "+docTag );
		    if ( docTag.compareTo("<DOCUMENT>") == 0 ) {
			word[wordLoc] = inChar;
			wordLoc++;
		    } else if ( docTag.compareTo("<DOCUMENT>\n<TYPE>GRAPHIC") == 0  ||
				docTag.compareTo("<DOCUMENT>\n<TYPE>ZIP"    ) == 0  ||
				docTag.compareTo("<DOCUMENT>\n<TYPE>EXCEL"  ) == 0   ) {
			state = st_BINARY;
			wordLoc = 0;
			// System.out.println("Found binary file:" + String.valueOf(word,12,9) );
		    } else  {
			wordLoc = 0;
			state = st_START;
		    }
		} else if ( wordLoc < maxWordSize ) {
		    word[wordLoc] = inChar;
		    wordLoc++;
		}
	    } else if ( state == st_BINARY ) {
		if ( inChar == '\n' ) {
		    if ( String.valueOf(word,0,wordLoc).compareTo("</DOCUMENT>") == 0 ) {
			state = st_START;
		    }
		    wordLoc = 0;
		} else {
		    if ( wordLoc < maxWordSize ) {
			word[wordLoc] = inChar;
			wordLoc++;
		    }
		}
	    }
	}
	if ( haveWord ) {
	    //	    return newWord;
	    return null;
	} else {
	    return null;
	}
    }


    public String nextWord() throws IOException {
	String  newWord = new String();
	boolean haveWord = false;
	int     charin   = 0;
	char    inChar;
	while ( !haveWord && (charin = isr.read()) != -1 ) {
	    inChar = (char)charin;
	    // System.out.println( "nextWord: state = "+Long.toString(state)+"  char = "+Character.toString(inChar));
	    if ( state == st_START ) {
		if ( inChar == '<' ) {
		    state = st_TAG;
		    wordLoc = 0;
		    word[wordLoc] = inChar;
		    wordLoc++;
		} else if ( Character.isLetter( inChar)) {
		    state = st_WORD;
		    wordLoc = 0;
		    word[wordLoc] = Character.toLowerCase(inChar);
		    wordLoc++;
		} else if ( Character.isSpace( inChar)) {
		    state = st_SPACE;
		} else if ( inChar == '&' ) {
		    state = st_SYMBOL;
		}
	    } else if ( state == st_TAG ) {
		if ( inChar == '>' ) {
		    // System.out.println("TAG: "+String.valueOf(word,0,wordLoc));
		    if ( String.valueOf( word, 0, wordLoc ).compareTo("<DOCUMENT") == 0 ) {
			state = st_DOCUMENT;
			word[wordLoc] = inChar;
			wordLoc++;
			// System.out.println("Found DOCUMENT: " + String.valueOf(word,0,wordLoc));
		    } else {
			wordLoc = 0;
			state = st_START;
		    }
		} else {
		    if ( wordLoc < maxWordSize ) {
			word[wordLoc] = inChar;
			wordLoc++;
		    }
		}
	    } else if ( state == st_WORD ) {
		if ( Character.isLetter( inChar) // ||
		     // Character.isDigit( inChar)  ||
		     //	inChar == '-'              ||
		     // inChar == '_'
		     ) {
		    if ( wordLoc < maxWordSize ) {
			word[wordLoc] = Character.toLowerCase(inChar);
			wordLoc++;
		    }
		} else if ( inChar == '<' ) {
		    if ( wordLoc >= minWordSize ) {
			stemmer.add( word, wordLoc );
			stemmer.stem();
			newWord =  stemmer.toString();
			haveWord = true;
		    }
		    wordLoc = 0;
		    word[wordLoc] = inChar;
		    wordLoc++;
		    state = st_TAG;
		} else if ( Character.isSpace( inChar)) {
		    if ( wordLoc >= minWordSize  ) {
			stemmer.add( word, wordLoc );
			stemmer.stem();
			newWord =  stemmer.toString();
			haveWord = true;
			wordLoc = 0;
		    }
		    state = st_SPACE;
		} else {
		    if ( wordLoc >= minWordSize  ) {
			stemmer.add( word, wordLoc );
			stemmer.stem();
			newWord =  stemmer.toString();
			haveWord = true;
			wordLoc = 0;
		    }
		    state = st_START;
		}
	    } else if ( state == st_SYMBOL ) {
		if ( inChar == ';' ) {
		    wordLoc = 0;
		    state = st_START;
		}
	    } else if ( state == st_SPACE ) {
		if ( inChar == '<' ) {
		    wordLoc = 0;
		    word[wordLoc] = inChar;
		    wordLoc++;
		    state = st_TAG;
		} else if ( Character.isLetter( inChar)) {
		    wordLoc = 0;
		    word[wordLoc] = Character.toLowerCase(inChar);
		    wordLoc++;
		    state = st_WORD;
		} else if ( Character.isSpace( inChar)) {
		    state = st_SPACE;
		} else if ( inChar == '&' ) {
		    wordLoc = 0;
		    word[wordLoc] = inChar;
		    wordLoc++;
		    state = st_SYMBOL;
		} else {
		    state = st_START;
		}
	    } else if ( state == st_DOCUMENT ) {
		if ( inChar == '\n' ) {
		    String docTag = String.valueOf(word,0,wordLoc);
		    // System.out.println("DOC: "+docTag );
		    if ( docTag.compareTo("<DOCUMENT>") == 0 ) {
			word[wordLoc] = inChar;
			wordLoc++;
		    } else if ( docTag.compareTo("<DOCUMENT>\n<TYPE>GRAPHIC") == 0  ||
				docTag.compareTo("<DOCUMENT>\n<TYPE>ZIP"    ) == 0  ||
				docTag.compareTo("<DOCUMENT>\n<TYPE>EXCEL"  ) == 0   ) {
			state = st_BINARY;
			wordLoc = 0;
			// System.out.println("Found binary file:" + String.valueOf(word,12,9) );
		    } else  {
			wordLoc = 0;
			state = st_START;
		    }
		} else if ( wordLoc < maxWordSize ) {
		    word[wordLoc] = inChar;
		    wordLoc++;
		}
	    } else if ( state == st_BINARY ) {
		if ( inChar == '\n' ) {
		    if ( String.valueOf(word,0,wordLoc).compareTo("</DOCUMENT>") == 0 ) {
			state = st_START;
		    }
		    wordLoc = 0;
		} else {
		    if ( wordLoc < maxWordSize ) {
			word[wordLoc] = inChar;
			wordLoc++;
		    }
		}
	    }
	}
	if ( haveWord ) {
	    return newWord;
	} else {
	    return null;
	}
    }
    
    public static void main(String argv[]) {

	try {
	    FileInputStream fis   = new FileInputStream( argv[0] );
	    ParseSECData    parser = new ParseSECData( fis );
	    String newWord;
	    //	    BufferedReader in     = new BufferedReader( isr );
	    while ( (newWord = parser.nextWord()) != null ) {
		System.out.println( newWord );
	    }

	    fis.close();
 
	} catch (Exception e) {
	    e.printStackTrace();
	}
  
    }
}
