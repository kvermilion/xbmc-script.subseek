
"""
Primative query implementations for tuple of strings based indices.

Assumption: index of form tuple-->string
Where the lhs tuples are all of same length
and all query tuples are shorter than that length (or same)

Used to implement queries.
"""

from types import StringType, UnicodeType
#import parameters

class Extremum:
    "An abuse of __cmp__ to get 'minimal' and 'maximal' elements"
    def __init__(self, cmpValue):
        self.cmpValue = cmpValue
        if not cmpValue in (1,-1):
            raise ValueError, "cmp value should be -1 or 1"
        
    def __cmp__(self, other):
        if isinstance(other, Extremum):
            return cmp(self.cmpValue, other.cmpValue)
        else:
            return self.cmpValue

MINIMUMOBJECT = Extremum(-1)
MAXIMUMOBJECT = Extremum(1)

# XXX can all three behaviours be safely emulated using perversions of "Range?"

class MatchTuple:
    """
    find seq of list(key+(value,)) where key->value in index and
    first part of key is an exact match for tuple.
    """
    def __init__(self, index, tuple):
#        parameters.alloc(self, "match")
        self.index = index
        self.tuple = tuple

#    def __del__(self):
#        parameters.dealloc(self)
        
    def estimate(self):
        """
        numeric estimate of result size
        """
        index = self.index
        startkey = self.firstKey()
        endkey = self.beyondEndKey()
        startIndex = index.indexOf(startkey)
        if endkey is None:
            endIndex = index.lastIndex()
        else:
            endIndex = index.indexOf(endkey)
        if startIndex>endIndex:
            raise ValueError, "start should not be smaller than end in estimate"
        return endIndex-startIndex
    
    def evaluateD(self, selectIndex=None, truncateSize=None):
        """
        generate the matching results as dictionary index->match.
        If selectIndex is set only return the key entries at that index (the ids, usually).
        """
        index = self.index
        fromKey = self.firstKey()
        toKey = self.beyondEndKey()
        #D = index.rangeDict(fromKey, toKey, truncateSize=truncateSize)
        (keys, values) = index.rangeLists(fromKey, toKey, truncateSize)
        #pr "pquery evaluateD gives", len(keys)
        #for k in keys:
        #    pr k
        count = 0
        result = {}
        if selectIndex is None:
            # I don't think this is ever used? (remove?)
            for matchkey in keys: #D.keys():
                #matchvalue = D[matchkey]
                matchvalue = values[count]
                r = list(matchkey)
                r.append(matchvalue)
                result[count] = r
                count += 1
        else:
            for matchkey in keys: #D.keys():
                r = matchkey[selectIndex]
                result[count] = r
                count += 1
        return result
    
#     def evaluateD0(self, selectIndex=None):
#         # aggregate this!
#         resultsD = {}
#         count = 0
#         # store all results in resultsD...
#         matchkey = self.firstKey()
#         #done = False
#         index = self.index
#         kv = index.findAtOrNextKeyValue(matchkey)
#         while kv:
#             (matchkey, matchvalue) = kv
#             if self.matches(matchkey):
#                 # key matches
#                 if selectIndex is None:
#                     r = list(matchkey)
#                     r.append(matchvalue)
#                     resultsD[count] = r
#                 else:
#                     r = matchkey[selectIndex]
#                     resultsD[count] = r
#                 count += 1
#                 kv = index.nextKeyValueAfter(matchkey)
#             else:
#                 kv = None # beyond the last match
#         return resultsD
    
    def evaluate(self):
        "same as evaluateD, but return a list"
        D = self.evaluateD()
        Dlen = len(D)
        r = xrange(Dlen)
        result = [None] * Dlen
        for i in r:
            result[i] = D[i]
        return result
    
    def matches(self, matchtuple):
        # aggregate this!
        # "Match.matches"
        # could be inlined for speed, maybe
        tup = self.tuple
        ntuple = len(tup)
        matchprefix = matchtuple[:ntuple]
        return matchprefix == tup
    def firstKey(self):
        return self.tuple
    def beyondEndKey(self):
        tup = self.tuple
        return tup+(MAXIMUMOBJECT,)
        
class PrefixTuple(MatchTuple):
    "first part of tuple must match and last entry must be a prefix"
    def matches(self, matchtuple):
        # could inline
        # "PrefixTuple.matches"
        tup = self.tuple
        n1 = len(tup)-1
        tuplen1 = tup[:n1]
        matchtuplen1 = matchtuple[:n1]
        if tuplen1!=matchtuplen1:
            # "first parts don't match %s %s" % (tuple, matchtuple)
            return False # first parts don't match
        tlast = tup[n1]
        mlast = matchtuple[n1]
        # "first parts match %s %s %s %s %s" % (
        #    tuple, matchtuple, mlast, tlast, mlast.startswith(tlast))
        return mlast.startswith(tlast)
    
    def beyondEndKey(self):
        tup = self.tuple
        n1 = len(tup) -1
        #tuplen1 = tup[:n1]
        last = tup[n1]
        if last=="":
            next = chr(0)
        else:
            lastchr = last[-1]
            lastord = ord(lastchr)
            tc = type(lastchr)
            nextord = lastord+1
            if tc is StringType:
                if nextord<256:
                    nextchr = chr(nextord)
                else:
                    nextchr = chr(256)*2
            elif tc is UnicodeType:
                #nextchr = lastchr + unichr(0x10fffd)
                if nextord<0x10fffd:
                    nextchr = unichr(nextord)
                else:
                    nextchr = unichr(0x10fffd)*2
            else:
                raise TypeError, "cannot use PrefixTuple with non string %s (from %s)" % (repr(last), tup)
            next = last[:-1] + nextchr
        return tup[:-1] + (next,)

class Range(MatchTuple):
    "match tuples in range from startTuple (inclusive) to endTuple (exclusive)"
    def __init__(self, index, startTuple, endTuple):
        MatchTuple.__init__(self, index, startTuple)
        if startTuple>endTuple:
            raise ValueError, "bad range endpoints"
        self.index = index
        self.startTuple = startTuple
        self.endTuple = endTuple
        
    def matches(self, matchTuple):
        startTuple = self.startTuple
        endTuple = self.endTuple
        return (startTuple<=matchTuple) and (matchTuple<endTuple)
    
    def firstKey(self):
        return self.startTuple
    
    def beyondEndKey(self):
        return self.endTuple

def test():
    # in memory test: no disk storage required
    #import kisstree
    #T = kisstree.TinyKTreeNoDups()
    import dframe
    T = dframe.DataFrame()
    D = {}
    # populate it
    for x in xrange(1, 1000, 13):
        sx = str(x)
        for y in xrange(1, 100, 44):
            sy = str(y)
            k = (sx, sy)
            v = sx+","+sy
            T[k] = v
            D[k] = v
    # do some matches
    M1 = MatchTuple(T, ("5",))
    est = M1.estimate()
    if est:
        raise ValueError, "estimate should be 0 for M1"
    result = M1.evaluate()
    if result:
        raise ValueError, "no values expected for M1"
    print "M1 ok", result
    M2 = MatchTuple(T, ("14",))
    est = M2.estimate()
    result = M2.evaluate()
    print "est, result", est, result
    checkResult(result, est, D, lower=("14",), upper=("14"+chr(0),))
    P1 = PrefixTuple(T, ("x",))
    est = P1.estimate()
    result = P1.evaluate()
    print "P1", est, result
    checkResult(result, est, D, lower=0, upper=0)
    P2 = PrefixTuple(T, ("1",))
    est = P2.estimate()
    result = P2.evaluate()
    print "P2", est, result
    checkResult(result, est, D, lower=("1",), upper=("1x",))
    R1 = Range(T, ("1",), ("1",))
    est = R1.estimate()
    result = R1.evaluate()
    print "R1", est, result
    checkResult(result, est, D, lower=0, upper=0)    
    R2 = Range(T, ("1",), ("4",))
    est = R2.estimate()
    result = R2.evaluate()
    print "R2", est, result
    checkResult(result, est, D, lower=("1",), upper=("4",))    
    R3 = Range(T, ("183","1"), ("248","4"))
    est = R3.estimate()
    result = R3.evaluate()
    print "R3", est, result
    checkResult(result, est, D, lower=("183","1"), upper=("248","4"))    

def checkResult(result, est, D, lower, upper):
    for k in D: #D.keys():
        v = D[k]
        L = list(k)
        L.append(v)
        t = tuple(L)
        if t>=lower and t<upper and L not in result:
            raise ValueError, "result not found "+repr(t)
    for x in result:
        t = tuple(x)
        if t<lower or t>=upper:
            raise ValueError, "found bad result %s" % repr(t)
        keypart = t[:-1]
        valuepart = t[-1]
        if D.get(keypart)!=valuepart:
            raise ValueError, "can't find %s in dictionary: found %s" % (repr(t), repr(valuepart))

if __name__=="__main__":
    test()

