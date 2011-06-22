
"""
Helper for making HTML annotations over strings.
"""
# XXX not particularly fast: KISS

import entry
import bisect
import specialValues
#import parameters

class Annotator:
    def __init__(self, text):
        # if text is a special value: get contents
        #parameters.alloc(self, ("ann", str(text)[:40]))
        if isinstance(text, specialValues.SpecialValue):
            text = text.content() # XXXX could be lazy about this...
        self.text = str(text)
        self.substitutionChars = []
        self._insertBefore = {}
        self._insertAfter = {}

    #def __del__(self):
    #    parameters.dealloc(self)
        
    def __str0__(self):
        # simple and slow implementation
        text = self.text
        subs = self.substitutionChars
        insertBefore = self._insertBefore
        insertAfter = self._insertAfter
        ltext = len(text)
        L = [None] * ltext
        for i in xrange(ltext):
            if subs.has_key(i):
                emitString = subs[i]
            else:
                emitString = text[i]
            if insertBefore.has_key(i):
                insertStrings = insertBefore[i]
                emitString = "".join(insertStrings) + emitString
            if insertAfter.has_key(i):
                insertStrings = insertAfter[i]
                emitString = emitString+ "".join(insertStrings)
            L[i] = emitString
        return "".join(L)

    def __str__(self):
        # more complicated, hopefully faster/cheaper implementation
        text = self.text
        ltext = len(text)
        breakD = {0:"", ltext: ""}
        substitutions = self.substitutionChars
        insertBefore = self._insertBefore
        insertAfter = self._insertAfter
        breakD.update(insertBefore)
        #breakD.update(insertAfter)
        for i in insertAfter:
            breakD[i+1] = insertAfter[i]
        breaks = breakD.keys()
        breaks.sort()
        Dout = {}
        count = 0
        cursor = 0
        for nextcursor in breaks:
            if nextcursor>cursor:
                # add segment from cursor to nextcursor
                segment = text[cursor:nextcursor]
                lastindex = nextcursor-1
                for (fromString, toString) in substitutions:
                    segment = segment.replace(fromString, toString)
                if insertBefore.has_key(cursor):
                    insertStrings = insertBefore[cursor]
                    segment = "".join(insertStrings) + segment
                if insertAfter.has_key(lastindex):
                    insertStrings = insertAfter[lastindex]
                    segment = segment + "".join(insertStrings)
                cursor = nextcursor
                Dout[count] = segment
                count += 1
        L = [None]*len(Dout)
        for i in Dout:
            L[i] = Dout[i]
        #pr "segments", L
        return "".join(L)

    def summarize(self):
        "return lineNo: text only for inserted lines"
        # determine indices of line breaks (including end of text)
        before = self._insertBefore
        after = self._insertAfter
        if not before and not after:
            return {} # shortcut
        text = self.text
        #pr "<hr><b>summarize</b><br>", repr(text), "<hr>"
        lsplit = text.split("\n")
        lineEnds = list(lsplit)
        cursor = 0
        for i in xrange(len(lsplit)):
            linetext = lsplit[i]
            linelength = len(linetext)
            newlineIndex = cursor+linelength
            cursor = newlineIndex+1
            lineEnds[i] = newlineIndex
            # check
            #pr "at", newlineIndex, len(text)
            if newlineIndex!=len(text) and text[newlineIndex] != "\n":
                raise ValueError, "logic bug: no newline at "+repr(text[newlineIndex])
        # find lines with changes
        changedLines = {}
        for D in (before, after):
            for location in D:
                lineIndex = bisect.bisect(lineEnds, location)
                changedLines[lineIndex] = location
        # DEBUG:
        for i in xrange(len(lsplit)):
            changedLines[lineIndex] = 1
        # extract the lines (assuming no newlines have been altered)
        strSelf = str(self)
        strSplit = strSelf.split("\n")
        for lineNo in changedLines.keys():
            changedLines[lineNo] = strSplit[lineNo]
        result = changedLines.items()
        result.sort()
        return result
    
    def __repr__(self):
        return repr(str(self))
    
    def quote(self):
        self.substitutionChars = [ ("&", "&amp;"), ("<", "&lt;"), (">", "&gt;") ]

    def quote0(self):
        "retired implementation"
        text = self.text
        subs = self.substitutionChars
        #pr "<hr>", repr(text), "<hr>"
        text = str(text)
        for i in xrange(len(text)):
            c = text[i]
            if c=="<":
                subs[i] = "&lt;";
            if c==">":
                subs[i] = "&gt;";
            if c=="&":
                subs[i] = "&amp;";
                
    def insertAfter(self, index, stuff):
        ltext = len(self.text)
        if index>=ltext or index<0:
            raise ValueError, "index not on string text "+repr(index)
        inserts = self._insertAfter
        iList = inserts.get(index)
        if iList is None:
            inserts[index] = iList = []
        iList.append(stuff)
        
    def insertBefore(self, index, stuff):
        ltext = len(self.text)
        if index>ltext or index<0:
            raise ValueError, "index not on string text "+repr(index)
        inserts = self._insertBefore
        iList = inserts.get(index)
        if iList is None:
            inserts[index] = iList = []
        iList.insert(0, stuff)

def Annotation(thing):
    "make into an annotator, if it's not one already"
    if isinstance(thing, Annotator):
        return thing
    result = Annotator(thing)
    result.quote()
    return result

def delimit(thing, markBefore, markAfter, ListOfStartEndPairs):
    #pr "<hr> thing=", repr(thing), "<hr>"
    result = Annotation(thing)
    for (startIndex, endIndex) in ListOfStartEndPairs:
        if endIndex<=startIndex:
            raise ValueError, "bad delimit marks -- start must be before end: "+repr((startIndex,endIndex))
        result.insertBefore(startIndex, markBefore)
        result.insertAfter(endIndex-1, markAfter)
    return result

def delimitMatches(thing, substring, markBefore, markAfter):
    if isinstance(thing, Annotator):
        realThing = thing.text
    else:
        realThing = str(thing)
    ListOfStartEndPairs = entry.findPrefixInText(substring, realThing)
    return delimit(thing, markBefore, markAfter, ListOfStartEndPairs)
        
if __name__=="__main__":
    #       012345678901234567890
    text = "this is a <b>test</b>"
    a = Annotator(text)
    a.quote()
    a.insertBefore(5, "<code>")
    a.insertAfter(8, "</code>")
    a.insertBefore(9, "<em>")
    a.insertAfter(20, "</em>")
    a.insertBefore(0, "<h1>")
    a.insertAfter(20, "</h1>")
    out = str(a)
    print "from", repr(text)
    print "got", repr(out)
    final = str(out)
    expected = '<h1>this <code>is a</code><em> &lt;b&gt;test&lt;/b&gt;</em></h1>'
    if final!=expected:
        raise ValueError, "final didn't match expected: "+repr((final, expected))
    text = "prefixinator \ncontaining\n prefix as prefixes several places"
    #import entry
    pairs = entry.findPrefixInText("prefix", text)
    d = delimit(text, "<b>", "</b>", pairs)
    print "from", repr(text)
    print "got", repr(d)
    expected = '<b>prefix</b>inator \ncontaining\n <b>prefix</b> as <b>prefix</b>es several places'
    if str(d)!=expected:
        raise ValueError, "delimit doesn't match expected "+repr((d, expected))
    print "summarize", d.summarize()
    a = d = None
