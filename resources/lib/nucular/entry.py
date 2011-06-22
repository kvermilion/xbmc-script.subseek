"""
Entry implementation for a conceptual data entry in a nucular data set.
"""

import re
#import bisect
import specialValues
import types
#import parameters

# XXX the implementation assumes that no mods are done after indexing

class Entry:
    "An association of attribute names to (multiple) values. Stored/indexed/retrieved by nucular"

    # special tags used for identities
    idAttribute = "i"
    indexD = None

    def __init__(self, identity, D=None):
        # D is mapping attr-->[values]
        # "D before", repr(D)
        #parameters.alloc(self, ("entry", identity))
        if D is None:
            D = {}
        idatr = self.idAttribute
        identry = [identity]
        if D.has_key(idatr):
            test = D[idatr]
            if test != identry:
                raise ValueError, "id for entry does not match id in dictionary "+repr((test,identry))
        else:
            D[idatr] = identry
        # "D after", repr(D)
        self.D = D
        self.myWordList = self.myWordCollection = None

    def __repr__(self):
        return "Entry(%s)" % self.D
    
    def asDictionary(self, strict=False):
        "represent contents as dictionary (fake it on multiple entries)."
        result = {}
        D = self.D
        for k in D:
            L = D[k]
            if len(L)==1:
                result[k] = L[0]
            elif len(L)>1:
                if strict:
                    raise ValueError, "strict: cannot convert to dictionary when attributes have multiple values: "+repr((k,L))
                try:
                    strs = map(str, L)
                except UnicodeEncodeError:
                    strs = map(unicode, L)
                result[k] = "\n".join(strs) # probably should be an error?
        return result
    
    def identity(self):
        "Return the identity string for this entry."
        idL = self.D[self.idAttribute]
        return idL[0]
    
#     def size(self):
#         "A somewhat arbitrary measure of entry complexity, down to character counts."
#         # UNUSED?
#         D = self.D
#         result = len(D)
#         for a in D:
#             vals = D[a]
#             result += len(vals)
#             for v in vals:
#                 result += len(str(v))
#         return result
    
    def toXML(self, indent=""):
        "Return an XML string representation for entry content."
        SV = specialValues.SpecialValue
        D = self.D
        idatr = self.idAttribute
        identity = self.identity()
        L = ['%s<entry id="%s">' % (indent, quote(identity))]
        ap = L.append
        innerindent = indent+"   "
        atts = D.keys()
        atts.sort()
        for a in atts:
            if a==idatr:
                continue
            vals = D[a]
            for v in vals:
                if isinstance(v, SV):
                    ap(v.xml(quote(a)))
                else:
                    ap('%s<fld n="%s">%s</fld>' % (innerindent, quote(a), quote(v)))
        ap(indent+"</entry>")
        return "\n".join(L)
    
    def compileWords(self):
        "Parse out the 'word' substrings in the entry content."
        if self.myWordList is not None:
            return # done already
        collection = self.words()
        list = self.words.keys()
        list.sort()
        self.myWordList = list
        self.myWordCollection = collection
        
    def attrDict(self, indexable=False, _instanceType=types.InstanceType):
        "return the attribute-->value List association for this entry."
        SV = specialValues.SpecialValue
        D = self.D
        if indexable:
            iD = self.indexD
            if iD is not None:
                return iD # cached
            # translate to indexable strings
            result = self.D.copy()
            for x in D:
                L = D[x]
                L0 = []
                for v in L:
                    # this inner loop is a bottleneck for large queries
                    if type(v) is types.InstanceType and isinstance(v, SV):
                        v = v.content()
                    L0.append(v)
                result[x] = L0
            #self.indexD = result # cache it! (disabled to save space!)
            return result
        return D
    
    def __setitem__(self, attribute, value):
        "Associate value to attribute."
        D = self.D
        L = self.D.get(attribute)
        if not L:
            D[attribute] = L = []
        if value not in L:
            L.append(value)
            
    def __getitem__(self, attribute):
        "Get the value for attribute (fake it if there are multiple values)."
        iD = self.indexD
        if iD is None:
            iD = self.attrDict(indexable=True)
        L = iD[attribute]
        if len(L)==1:
            return L[0]
        else:
            strs = map(str, L)
            return "\n".join(strs) # should be an error?

    def sample(self, attribute, limit=3):
        "return a sample word for attribute if present"
        iD = self.indexD
        if iD is None:
            iD = self.attrDict(indexable=True)
        if iD.has_key(attribute):
            values = self[attribute]
            L = parseWords(values)
            lL = len(L)
            if lL:
                start = int(lL/2)
                result = []
                # XXXX funny heuristic improvised off the cuff
                for i in range(start, lL):
                    x = L[i]
                    if len(x)>2:
                        result.append(x)
                        if len(result)>=limit:
                            break
                #pr "samples", (attribute, result)
                return result
        return []
        
    def setValues(self, attribute, values):
        "assign values list to attribute"
        if attribute==self.idAttribute:
            if len(values)!=1:
                raise ValueError, "only one id permitted"
        self.D[attribute] = values
        
    def getValues(self, attribute):
        "get values list for attribute"
        test = self.D.get(attribute)
        if test:
            return test
        return []
    
    MINWORDLENGTH = 2
    
    def words(self, attribute=None, collection=None, attributeWords=None, attributeTranslation=None):
        "Find words in content and words associated with each attribute."
        # XXXX if attribute=None and attributeWords is set, collection
        # XXXX is unneeded (implicit from attributeWord)
        if attributeTranslation is None:
            attributeTranslation = {}
        minlen = self.MINWORDLENGTH
        #D = self.D
        D = self.attrDict(indexable=True)
        if collection is None:
            collection = {}
        if attribute is None:
            for attr in D.keys():
                collection = self.words(attr, collection, attributeWords, attributeTranslation)
        else:
            values = D.get(attribute, [])
            translation = attributeTranslation.get(attribute, attribute)
            for v in values:
                #v = str(v).lower()
                words = parseWords(v)
                ##pr "words = ", words
                for w in words:
                    if len(w)>minlen:
                        collection[w] = translation
                        if attributeWords is not None:
                            attrDict = attributeWords.get(translation)
                            if attrDict is None:
                                attrDict = {}
                            attrDict[w] = translation
                            attributeWords[translation] = attrDict
        return collection
    
    def wordStats(self, attributeTranslation=None):
        "Get collection of words and attribute/word associations."
        if attributeTranslation is None:
            attributeTranslation = {}
        freeCollection = {}
        attributeWords = {}
        self.words(None, freeCollection, attributeWords, attributeTranslation)
        return (freeCollection, attributeWords)

# XXX in some future version the handling of word parsing might
# XXX be encapsulated in an object...

# need to make this a parameter of some kind
RE = re.compile("\w+")
WORDLENGTHLIMIT = 30

def parseWords(v, RE=RE, limit=WORDLENGTHLIMIT):
    if type(v) not in types.StringTypes:
        v = str(v)
    v = v.lower()
    result = RE.findall(v)
    result = [ x[:WORDLENGTHLIMIT] for x in result ]
    return result

def findPrefixInText(prefix, text, RE=RE):
    "return [(start,end), ...] for occurrances of prefix in text"
    lprefix = len(prefix)
    result = []
    cursor = 0
    try:
        text = text.lower()
    except:
        text = str(text).lower()
    while cursor>=0:
        match = RE.search(text, cursor)
        if match is None:
            cursor = -1 # all done
        else:
            mstart = match.start()
            mend = match.end()
            mtext = text[mstart:mend]
            if mtext.startswith(prefix):
                result.append( (mstart, mstart+lprefix) )
            cursor = mend
    return result

def suggestCompletionInText(prefix, text, RE=RE):
    "return a single completion suggestion starting with prefix from text"
    lprefix = len(prefix)
    result = []
    cursor = 0
    try:
        text = text.lower()
    except:
        text = str(text).lower()
    while cursor>=0:
        match = RE.search(text, cursor)
        if match is None:
            cursor = -1 # all done
        else:
            mstart = match.start()
            mend = match.end()
            mtext = text[mstart:mend]
            result = mtext
            if mtext.startswith(prefix):
                lmtext = len(mtext)
                if lmtext==lprefix:
                    # exact match: include the next word
                    nextmatch = RE.search(text, mend)
                    if nextmatch is not None:
                        nstart = nextmatch.start()
                        nend = nextmatch.end()
                        ntext = text[nstart:nend]
                        #pr "return this word followed by next word"
                        result = "%s %s" % (mtext, ntext)
                        #raise "debug", "appended result "+repr(result)
                else:
                    #pr "prefix match: include just the rest of the word"
                    result = mtext
                if result!=prefix:
                    #pr "for", prefix, "completing", result
                    return result
            cursor = mend
    #raise ValueError, "could not find prefix in text: "+repr((prefix, text[40:]))
    #pr "prefix", prefix, "not found in", repr(text[:40])
    return None # no suggestion from this value

def findPrefixInLines(prefix, text, RE=RE):
    "return [ (line, [(start,end), ...]), ...] for occurances of prefix in lines of text."
    lines = str(text).split("\n")
    return [ (line, findPrefixInText(prefix, line, RE)) for line in lines ]

def quote(x):
    "xml string quoting"
    # XXX if this is in a library someplace I can't find it.
    if type(x) not in types.StringTypes:
        x = str(x)
    x = x.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return x

def EntryFromXMLNode(elt):
    "derive an entry from an ElementTree node"
    if hasattr(elt, "tag") and elt.tag=="entry":
        identity = elt.attrib.get("id")
        #pr "id", identity
        if not identity:
            raise ValueError, "entry must have non empty id"
        thisEntry = Entry(identity)
        fcount = 0
        for fld in elt.getchildren():
            if hasattr(fld, "tag") and fld.tag=="fld":
                fcount += 1
                fname = fld.attrib.get("n")
                fval = fld.text
                #pr "fld", (fname, fval)
                thisEntry[fname] = fval
        return thisEntry
    else:
        return None # fail (maybe a comment)

def EntryFromXMLText(text):
    "Derive an entry from XML text."
    from findetree import etree
    node = etree.fromstring(text)
    return EntryFromXMLNode(node)

def EntriesFromXMLNode(tree):
    "Derive a sequence of entries from ElementTree node."
    result = {}
    if tree.tag!="entries":
        return None # fail, wrong node type
    for elt in tree.getchildren():
        thisEntry = EntryFromXMLNode(elt)
        if thisEntry is not None:
            identity = thisEntry.identity()
            if result.has_key(identity):
                raise ValueError, "repeated identity is not permitted "+repr(identity)
            result[identity] = thisEntry
    if not result:
        raise ValueError, "no entry elements found in XML"
    return result.values()

def EntriesFromXMLText(text):
    "Derive a sequence of entries from XML text."
    from findetree import etree
    node = etree.fromstring(text)
    result = EntriesFromXMLNode(node)
    if result is None:
        # try to parse as a single entry
        test = EntryFromXMLNode(node)
        if test:
            result = [test]
    return result
