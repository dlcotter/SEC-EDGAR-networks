import collections
import csv
import io
import re
import sys

class SECDict(dict):
    def __missing__(self,key):
        return ""
    
def toDate(str):
    if len(str) > 0:
        return str[0:4]+'-'+str[4:6]+'-'+str[6:8]
    else:
        return "1990-01-01"

class SECFiling:
    """An SEC Filing"""

    
    objType = "SECFiling"
    secHeaderStartRE   = re.compile("<SEC-HEADER>",re.MULTILINE)
    secHeaderEndRE     = re.compile("</SEC-HEADER>",re.MULTILINE)
    secDocumentStartRE = re.compile("<DOCUMENT>",re.MULTILINE)
    secDocumentEndRE   = re.compile("</DOCUMENT>",re.MULTILINE)
    secFilenameRE      = re.compile("<FILENAME>",re.MULTILINE)
    secTextStartRE     = re.compile("<TEXT>",re.MULTILINE)
    secTextEndRE       = re.compile("</TEXT>",re.MULTILINE)
    secXMLStartRE      = re.compile("<XML>",re.MULTILINE)
    secXMLEndRE        = re.compile("</XML>",re.MULTILINE)

    global Token
    Token = collections.namedtuple( 'Token', ['type','value'])
    
    def __init__(self, accessionNumber, submissionType, docCount, reportingDate, filingDate, changeDate ):
        self.objType = SECFiling.objType
        self.accessionNumber = accessionNumber
        self.submissionType = submissionType
        self.docCount = docCount
        self.reportingDate = reportingDate
        self.filingDate = filingDate
        self.changeDate = changeDate

    def setAccessionNumber(self, accessionNumber):
        self.accessionNumber = accessionNumber
            
    def subType(self, subType):
        self.subType = subType

    def docCount(self, docCount):
        self.docCount = docCount

    def reportingDate(self, reportingDate):
        self.reportingDate = reportingDate

    def filingDate(self, filingDate):
        self.filingDate = filingDate

    def changeDate(self, changeDate):
        self.changeDate = changeDate

    def toCSV(self,csvWriter):
        csvWriter.writerow([self.objType,
                            self.accessionNumber,
                            self.submissionType,
                            self.docCount,
                            self.reportingDate,
                            self.filingDate,
                            self.changeDate])

        
    def parseHeader(hdrContent):
        global parseHeader
        tags = [
            ('accessionNumber', r'ACCESSION NUMBER:' ),
            ('businessAddress', r'BUSINESS ADDRESS:'),
            ('changeDate',      r'DATE AS OF CHANGE:'),
            ('cik',             r'CENTRAL INDEX KEY:'),
            ('city',            r'CITY:'),
            ('companyData',     r'COMPANY DATA:'),
            ('companyName',     r'COMPANY CONFORMED NAME:'),
            ('documentCount',   r'PUBLIC DOCUMENT COUNT:'),
            ('filingData',      r'FILING VALUES:'),
            ('filingDate',      r'FILED AS OF DATE:'),
            ('fiscalYearEnd',   r'FISCAL YEAR END:'),
            ('issuer',          r'ISSUER:'),
            ('mailAddress',     r'MAIL ADDRESS:'),
            ('ownerData',       r'OWNER DATA:' ),
            ('phone',           r'BUSINESS PHONE:'),
            ('reportingOwner',  r'REPORTING-OWNER:'),
            ('reportingPeriod', r'CONFORMED PERIOD OF REPORT:'),
            ('sic',             r'STANDARD INDUSTRIAL CLASSIFICATION:'),
            ('state',           r'STATE:'),
            ('stateOfInc',      r'STATE OF INCORPORATION:'),
            ('street1',         r'STREET 1:'),
            ('submissionType',  r'CONFORMED SUBMISSION TYPE:'),
            ('tradingSymbol',   r'TRADING SYMBOL:'),
            ('zip',             r'ZIP:'),
            ('value',           r'([A-Za-z0-9-&/ \[\]]+)$'),

        ]
        values = SECDict()
        state = 0
        kind  = ""
        lastkind = ""
        currentAccessionNumber = ""
        tag_regex = '|'.join('(?P<%s>%s)' % pair for pair in tags)
#        print("tag_regex: "+tag_regex )
        for mo in re.finditer( tag_regex, hdrContent, re.MULTILINE|re.ASCII ):
            kind     = mo.lastgroup
            startLoc = mo.end()
            v        = mo.group(0)
#            print("parseHeader: state:"+str(state)+"  kind="+kind+"  lastkind="+lastkind+"  v="+v+"  startLoc = "+str(startLoc)) # +": ("+hdrContent[startLoc:(startLoc+20)]+")")
            if kind == 'value':
                values[lastkind] = v
            elif kind == 'issuer':
                state = 10
                if 'accessionNumber' in values:
                    filingDate = toDate(values.get('filingDate',''))
                    yield [[ "filings",
                             values['accessionNumber'],
                             values.pop('submissionType'),
                             values.pop('documentCount'),
                             toDate(values.pop('reportingPeriod')),
                             filingDate,
                             toDate(values.pop('changeDate'))]]
            elif kind == 'businessAddress':
                if state == 10:
                    state = 11
                    sicNumber = ''
                    sic = values.pop('sic','')
                    if sic:
                        mo = re.search(r'\[(\d+)\]',sic)
                        if mo:
                            sicNumber = mo.group(1)
                        yield [[ 'filings-entities',
                                 values['accessionNumber'],
                                 values['cik']],
                               [ 'entities',
                                 values['cik'],
                                 values.pop('tradingSymbol',''),
                                 values.pop('companyName',''),
                                 'issuer',
                                 values.pop('irsNumber',''),
                                 sic,
                                 sicNumber,
                                 values.pop('stateOfInc',''),
                                 values.pop('fiscalYearEnd','') ]]
                elif state == 21:
                    yield [[ 'filings-entities',
                             values['accessionNumber'],
                             values['cik']],
                           [ 'entities',
                             values['cik'],
                             '',
                             values.pop('companyName',''),
                             'owner',
                             '',
                             '',
                             '',
                             '',
                             '' ]]
            elif kind == 'mailAddress':
                if state == 11:
                    state = 12
                    yield [['contacts',
                            values['cik'],
                            filingDate,
                            'issuer',
                            values.get('street1',''),
                            values.get('street2',''),
                            values.get('street3',''),
                            values.get('city',''),
                            values.get('state',''),
                            values.get('zip',''),
                            values.get('phone','')]]
                elif state == 21:
                    state = 22
                    yield [['contacts',
                            values['cik'],
                            filingDate,
                            'owner',
                            values.get('street1',''),
                            values.get('street2',''),
                            values.get('street3',''),
                            values.get('city',''),
                            values.get('state',''),
                            values.get('zip',''),
                            values.get('phone','')]]
            elif kind == 'reportingOwner':
                if state == 12:
                    state = 20
                    yield [['contacts',
                            values['cik'],
                            filingDate,
                            'issuer',
                            values.get('street1',''),
                            values.get('street2',''),
                            values.get('street3',''),
                            values.get('city',''),
                            values.get('state',''),
                            values.get('zip',''),
                            values.get('phone','')]]
            elif kind == 'ownerData':
                state = 21
            lastkind = kind
        if state == 22:
            yield [['contacts',
                    values['cik'],
                    filingDate,
                    'owner',
                    values.get('street1',''),
                    values.get('street2',''),
                    values.get('street3',''),
                    values.get('city',''),
                    values.get('state',''),
                    values.get('zip',''),
                    values.get('phone','')]]
        
    def parse(fileName,csvWriter):
        with open( fileName, "r" ) as inFile:
            fileContent = inFile.read()
            inFile.close()
            match = SECFiling.secHeaderStartRE.search(fileContent)
            if match is not None:
                headerStart = match.start(0)
                match = SECFiling.secHeaderEndRE.search(fileContent, headerStart+12)
                if match is not None:
                    headerEnd = match.start(0)
#                    print("Header content found at: "+str(headerStart)+" - "+str(headerEnd))
                    headerContent = fileContent[headerStart:headerEnd]
#                    print( "Header:")
#                    print( headerContent )
                    for obj in SECFiling.parseHeader(headerContent):
                        csvWriter.writerows(obj)
            
if __name__ == "__main__":
    with io.StringIO("",newline='\n') as csvStrings:
        csvWriter = csv.writer(csvStrings, delimiter='|',quoting=csv.QUOTE_MINIMAL )
        SECFiling.parse( sys.argv[1], csvWriter)
        print(csvStrings.getvalue())
