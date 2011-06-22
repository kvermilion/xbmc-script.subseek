
"""
Primative filter:

select entry identities based on a selection criterion
(used to implement queries).
"""

import entry
from stringAnnotator import delimitMatches
#from types import UnicodeType, StringType

# XXXX eventually will need richer type support.
# XXXX eventually will want to avoid splitting some attributes...?

# HACKY! the subscript for IDs in all indexes of interest is -1
IDINDEX = -1

class Filter:
    "container for a related query and match test, assumed to correspond"
    def __init__(self, query, test, idIndex=IDINDEX):
        self.query = query
        self.test = test
        self.idIndex = idIndex

    def toXML(self):
        return self.test.toXML()
    
    def estimate(self):
        query = self.query
        if query is None:
            return None
        return query.estimate()
    
    def evaluateD(self, truncateSize=None):
        query = self.query
        if query is None:
            raise ValueError, "cannot evaluate: no query for "+repr(self.test)
        D = self.query.evaluateD(selectIndex=self.idIndex, truncateSize=truncateSize)
        result = {}
        for ident in D.values():
            result[ident] = ident
        return result
    
    def evaluate(self):
        result = self.evaluateD().values()
        #result.sort()
        return result

    def annotate(self, dictionary, startmark, endmark):
        "annotate matching part of dictionary contents, delimiting with start and end mark"
        return self.test.annotate(dictionary, startmark, endmark)

    def SuggestionFilter(self):
        "translate to a filter appropriate for generating suggested completions"
        return Filter(self.query, self.test.SuggestionTest(), self.idIndex)

    def suggestion(self, dictionary, allFields, byFields):
        return self.test.suggestion(dictionary, allFields, byFields)
    
    #def matches(self, description): -- not used
    #    return self.test.matches(description)

# XXXX disjunction and conjunction implementation not complete yet
#
# class DisjunctionFilter:
#     "disjunction of several queries"
#     def __init__(self, filters):
#         self.filters = filters
        
#     def estimate(self):
#         result = 0
#         for f in self.filters:
#             result += f.estimate()
#         return result
    
#     def evaluateD(self, selectIndex=None, truncateSize=None):
#         result = {}
#         for f in self.filters:
#             d = f.evaluateD(selectIndex=selectIndex, truncateSize=truncateSize)
#             result.update(d)
#         return result
    
#     def evaluate(self):
#         result = self.evaluateD().values()
#         #result.sort()
#         return result

#     def annotate(self, dictionary, startmark, endmark):
#         "not implemented"

# class ConjunctionFilter(DisjunctionFilter):
#     # in this case use summation estimate ala disjunction, because that's how much work is required.
#     def evaluateD(self, selectIndex=None, truncateSize=None):
#         result = None
#         # no real attempt to optimize this
#         for f in self.filters:
#             fD = f.evaluateD(selectIndex=selectIndex, truncateSize=truncateSize)
#             if result is None:
#                 result = fD
#             else:
#                 for k in result.keys(): # result modified
#                     if not fD.has_key(k):
#                         del result[k]
#         if result is None:
#             return {} # should be an error?
#         return result


# XXXX NEED TO AGGREGATE THE __CALL__ TESTS OVER DICT OF ID->ENTRY
# XXXX NEED TO ADD CONJUNCTIONTEST, DISJUNCTIONTEST

class AttributeMatchTest:
    "test whether an attribute value matches a given value"
    def __init__(self, attr, value):
        self.attr = attr
        self.value = value

    def SuggestionTest(self):
        "translate to a test which will generate a suggestion for a partial match"
        return AttributePrefixTest(self.attr, self.value)

    def suggestion(self, dictionary, allFields, byFields):
        raise ValueError, "AttributeMatchTest cannot directly generate a suggestion, sorry"

    def annotate(self, dictionary, startmark, endmark):
        "from a dictionary representation for an entry, generate a new dictionary with annotations added at matches for self"
        result = dictionary.copy()
        attr = self.attr
        value = self.value
        L = dictionary.get(attr)
        if L:
            result[attr] = [ delimitMatches(v, value, startmark, endmark) for v in L ]
        return result
        
    def toXML(self):
        "return XML representation for self"
        return '<match n="%s" v="%s"/>' % (self.attr, self.value)
    
    def __call__(self, entry):
        "determine whether entry matches self"
        #description = entry.D
        description = entry.attrDict(indexable=True)
        L = description.get(self.attr)
        if L is None:
            return False
        value = self.value
        for v in L:
            if v==value:
                return True
        return False

class AttributePrefixTest:
    "test whether an attribute value has a given prefix."
    def __init__(self, attr, value):
        self.attr = attr
        self.value = value

    def suggestion(self, dictionary, allFields, byFields):
        value = self.value
        attr = self.attr
        L = dictionary.get(attr)
        for v in L:
            s = entry.suggestCompletionInText(value, v)
            if s:
                D = byFields.get(attr, {})
                D[s] = v
                byFields[attr] = D
                return {attr: s}
        #raise ValueError, "no completion found for "+repr(value)
        return None

    def SuggestionTest(self):
        "translate to a test which will generate a suggestion for a partial match"
        return self
        
    def annotate(self, dictionary, startmark, endmark):
        result = dictionary.copy()
        attr = self.attr
        value = self.value
        L = dictionary.get(attr)
        if L:
            result[attr] = [ delimitMatches(v, value, startmark, endmark) for v in L ]
        return result
        
    def toXML(self):
        return '<prefix n="%s" p="%s"/>' % (self.attr, self.value)
    
    def __call__(self, entry):
        #description = entry.D
        description = entry.attrDict(indexable=True)
        L = description.get(self.attr)
        if L is None:
            return False
        value = self.value
        for v in L:
            if v.startswith(value):
                return True
        return False

class AttributeRangeTest:
    "test whether an attribute value lies in a given alpha range."
    def __init__(self, attr, low, high):
        self.attr = attr
        self.low = low
        self.high = high

    def SuggestionTest(self):
        "translate to a test which will generate a suggestion for a partial match"
        return AttributePrefixTest(self.attr, self.low) # XXX ???

    def suggestion(self, dictionary, allFields, byFields):
        raise ValueError, "AttributeRangeTest cannot directly generate a suggestion, sorry"
        
    def annotate(self, dictionary, startmark, endmark):
        result = dictionary.copy()
        attr = self.attr
        low = self.low
        high = self.high
        L = dictionary.get(attr)
        if L:
            L2 = []
            for v in L:
                if v>=low and v<=high:
                    L2.append( delimitMatches(str(v), str(v), startmark, endmark))
                else:
                    L2.append(v)
            result[attr] = L2
        return result
                
    def toXML(self):
        return '<range n="%s" low="%s" high="%s"/>' % (self.attr, self.low, self.high)
    
    def __call__(self, entry):
        #description = entry.D
        description = entry.attrDict(indexable=True)
        L = description.get(self.attr)
        if L is None:
            return False
        low = self.low
        high = self.high
        for v in L:
            if v>=low and v<=high:
                return True
        return False

class AttributeWordPrefixTest:
    "test if an entry contains a word as a word prefix in specified attribute."
    def __init__(self, attr, word, splitter):
        self.attr = attr
        self.word = word
        self.splitter = splitter

    def suggestion(self, dictionary, allFields, byFields):
        # XXX copied from APT above
        value = self.word
        attr = self.attr
        L = dictionary.get(attr)
        for v in L:
            s = entry.suggestCompletionInText(value, v)
            if s is not None:
                D = byFields.get(attr, {})
                D[s] = v
                byFields[attr] = D
                return {attr: s}
        #raise ValueError, "no completion found for "+repr(value)
        return None

    def SuggestionTest(self):
        "translate to a test which will generate a suggestion for a partial match"
        return self
        
    def annotate(self, dictionary, startmark, endmark):
        result = dictionary.copy()
        attr = self.attr
        value = self.word
        L = dictionary.get(self.attr)
        if L:
            result[attr] = [ delimitMatches(v, value, startmark, endmark) for v in L ]
        return result
        
    def toXML(self):
        return '<contains n="%s" p="%s"/>' % (self.attr, self.word)
    
    def __call__(self, entry):
        #description = entry.D
        description = entry.attrDict(indexable=True)
        #pr "testing", description, "looking in", self.attr, "for", self.word
        L = description.get(self.attr)
        if L is None:
            return False
        word = self.word
        splitter = self.splitter
        minlength = entry.MINWORDLENGTH
        for v in L:
            splitv = splitter(v)
            for vword in splitv:
                if len(vword)>minlength and vword.startswith(word):
                    return True
                #pr repr(vword), "too short or doesn't start with", repr(word)
        return False

class WordPrefixTest:
    "test if an entry contains a word as a word prefix in any attribute."
    def __init__(self, word, splitter):
        self.word = word
        self.splitter = splitter

    def suggestion(self, dictionary, allFields, byFields):
        value = self.word
        for a in dictionary:
            L = dictionary.get(a)
            for v in L:
                s = entry.suggestCompletionInText(value, v)
                if s is not None:
                    D = allFields.get(a, {})
                    D[s] = v
                    allFields[a] = D
                    # XXX possibly add to byFields also?
                    return {a:s}
        #raise ValueError, "no completion found for "+repr(value)
        return None

    def SuggestionTest(self):
        "translate to a test which will generate a suggestion for a partial match"
        return self

    def annotate(self, dictionary, startmark, endmark):
        result = dictionary.copy()
        value = self.word
        for a in dictionary:
            L = dictionary.get(a)
            #pr "<hr>   a,L =", (a,L), "<hr>"
            result[a] = [ delimitMatches(v, value, startmark, endmark) for v in L ]
        return result
        
    def toXML(self):
        return '<contains p="%s"/>' % (self.word,)
    
    def __call__(self, entry):
        #description = entry.D
        description = entry.attrDict(indexable=True)
        splitter = self.splitter
        word = self.word
        #attrs = description.keys()
        # "description = ", description
        minlength = entry.MINWORDLENGTH
        for attr in description.keys(): #attrs:
            L = description[attr]
            for v in L:
                splitv = splitter(v)
                for vword in splitv:
                    if len(vword)>minlength and vword.startswith(word):
                        return True
        return False

class ProximateTest:
    "test for proximate words in any attribute"
    def __init__(self, words, limit, splitter):
        if not words:
            raise ValueError, "some words are required! "+repr(words)
        self.words = words
        self.limit = limit
        self.splitter = splitter
        
    def suggestion(self, d, a, b):
        raise ValueError, "proximate test cannot make suggestions"
    
    def SuggestionTest(self):
        lastWord = self.words[-1]
        # just guessing.... Don't know how well this will work
        return WordPrefixTest(lastWord, self.splitter)
    
    def annotate(self, dictionary, startmark, endmark):
        # don't make any annotations for now
        return dictionary
    
    def toXML(self):
        return '<near limit="%s" words="%s"/>' % (self.limit, " ".join(self.words))

    # slow implementation:
#     def __call0__(self, entry):
#         description = entry.attrDict(indexable=True)
#         splitter = self.splitter
#         words = self.words
#         limit = self.limit
#         minlength = entry.MINWORDLENGTH
#         for attr in description.keys():
#             L = description[attr]
#             for v in L:
#                 splitv = splitter(v)
#                 for wordIndex in xrange(len(splitv)):
#                     if proximateMatch(splitv, wordIndex, words, limit, minlength):
#                         return True
#         return False

    # faster:
    def __call__(self, entry):
        description = entry.attrDict(indexable=True)
        splitter = self.splitter
        words = self.words
        limit = self.limit
        minlength = entry.MINWORDLENGTH
        for attr in description.keys():
            L = description[attr]
            for v in L:
                if v:#
                    v = v.lower()
                    if quickProximateFilter(v, words):
                        splitv = splitter(v)
                        if proximateMatchAnywhere(splitv, words, minlength, limit):
                            return True
        return False

def quickProximateFilter(text, queryWords, chunksize=100, deltasize=20):
    "return true if word occur in text in correct order anywhere, else false (easy elimination)"
    cursor = -1
    for word in queryWords:
        #if type(text) is UnicodeType or type(word) is UnicodeType:
        #    try:
        #        text = unicode(text)
        #        word = unicode(word)
        #    except UnicodeDecodeError:
        #        return False
        cursor = text.find(word, cursor+1)
        if cursor<0:
            #pr "didn't find", repr(word), "in", repr(text), "<br>"
            return False
    #pr "found", queryWords, "in", repr(text), "<br>"
    # now try harder: query must be within the same chunksize of text
    firstword = queryWords[0]
    remainder = range(1,len(queryWords))
    firstcursor = 0
    searchcursor = -1
    while firstcursor>=0:
        firstcursor = text.find(firstword, searchcursor+1)
        searchcursor = firstcursor
        tooFarAway = firstcursor + chunksize
        if firstcursor<0:
            # no appropriate chunk found
            return False
        nextcursor = firstcursor
        for i in remainder:
            nextword = queryWords[i]
            nextcursor = text.find(nextword, nextcursor)
            if nextcursor<0:
                return False # no appropriate chunk found
            if nextcursor>tooFarAway:
                break # this span exceeds chunk size
            tooFarAway += deltasize # look a little further for next word
        # if we got here and nextcursor doesn't exceed span: great!
        if nextcursor<=tooFarAway:
            #pr "ok span", repr(text[firstcursor:nextcursor+len(nextword)]), "<br>"
            return True
    # all spans exceed chunksize
    return False

def proximateMatchAnywhere(splitWords, queryWords, minLength, limit, fromSplitIndex=0, toSplitIndex=None, queryIndex=0):
    #pr "pma", splitWords[fromSplitIndex:toSplitIndex], queryWords[queryIndex:], "minlength=", minLength
    nsplit = len(splitWords)
    nquery = len(queryWords)
    if toSplitIndex is None:
        toSplitIndex = nsplit
    else:
        toSplitIndex = min(toSplitIndex, nsplit)
    queryWord = queryWords[queryIndex]
    nextQueryIndex = queryIndex+1
    for i in xrange(fromSplitIndex, toSplitIndex):
        wordi = splitWords[i]
        #pr i, queryIndex, "word is", repr(wordi), "queryWord is", repr(queryWord)
        if len(wordi)>minLength and wordi.startswith(queryWord):
            #pr i, repr(wordi), "matches", repr(queryWord), "limit", limit, "<br>"
            if nquery<=nextQueryIndex:
                # match!
                return True
            else:
                test = proximateMatchAnywhere(splitWords, queryWords, minLength, limit, i+1, i+limit+2, nextQueryIndex)
                if test:
                    return test
    return False

def pmaTest():
    splitWords = "this is a test".split()
    words = "is test".split()
    if not proximateMatchAnywhere(splitWords, words, 1, 3):
        raise ValueError, "no match for first test"
    words = "test this".split()
    if proximateMatchAnywhere(splitWords, words, 1, 30):
        raise ValueError, "match on second test"

def proximateMatch(splitWords, index, words, limit, minlength):
    "bad implementation!"
    # kiss with theoretical very bad worst case performance: who cares?
    thisWord = splitWords[index]
    testword = words[0]
    #pr "prox", (index, thisWord, testword)
    if len(thisWord)>minlength and thisWord.startswith(testword):
        # first word matches: possible match: test the other words...
        #pr "matched!"
        remainingWords = words[1:]
        if remainingWords:
            #pr "now checking ", remainingWords
            # test the remaining words
            indexLimit = min(len(splitWords), index+limit+1)
            for nextindex in range(index+1, indexLimit):
                test = proximateMatch(splitWords, nextindex, remainingWords, limit, minlength)
                if test:
                    return True # match found for remaining words.
            return False # no match found for remaining words
        else:
            # no remaining words: match!
            #pr "done! with success"
            return True # (base case of recursion)
    else:
        # first word doesn't match: not a match
        #pr "first words don't match", (thisWord, testword)
        return False


def proximateTest():
    splitWords = list("aabbaaccaaaaaaccaadd")
    #                  01234567890123456789
    for (words, index, limit, expected) in [ ("a", 0, 1, True),
                                      ("b", 2, 1, True),
                                      ("ab", 1, 1, True),
                                      ("bc", 3, 0, False),
                                      ("bd", 3, 4, False),
                                      ("bc", 3, 4, True),
                                      ]:
        wL = list(words)
        test = proximateMatch(splitWords, index, wL, limit, 0)
        if test!=expected:
            raise ValueError, "on %s expected %s, got %s" % ((words, limit, expected), expected, test)
    print "proximateTest complete with no errors"


if __name__=="__main__":
    proximateTest()
