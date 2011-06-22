
"""
data frame: store a limited amount of data in switchable representations.
In ideal circumstances this structure can be used as a "black box" for
shuttling collections of data here and there without looking inside...

marshal data is switchable between dictionary and (keys, values).
"""

TESTDIR = "../testdata/"

COMPRESS = False

import marshal
import cmarshal
import frameGenerators

# if enabled using cmarshal seems to slow some builds by about 1/4
# but index sizes shrink to 1/4 the size too.  I'm not sure whether
# query performance change is significant.
#
if COMPRESS:
    marshal = cmarshal
    
#import shuffle
import bisect
import types

def DataFrameFromLists(keys, values, sorted=True, check=False):
    "keys assumed sorted"
    #pr "data frame from lists", len(keys), len(values)
    if check and sorted:
        lastkey = None
        for k in keys:
            if lastkey is not None and k<=lastkey:
                raise ValueError, "bad key order "+repr((lastkey,k))
            lastkey = k
    result = DataFrame()
    result.D = None
    result.keys = keys
    result.values = values
    return result

class DataFrame:
    """
    Collection of key/value pairs for storage.
    Representation switches between sorted lists, dictionary, and marshal string.
    Ideally in many cases the data can stay marshalled when boundaries/size are known
    with no unpack (a crucial memory/time optimization).
    """
    # invariant: at any point any representation which is not None is valid
    
    writeable = True
    filename = None
    _freeze = False # for debugging
    _recordSeek = None # external seek index for larger record (for frameGenerators)
    endSeek = None
    
    def __init__(self, filename=None):
        "create an empty frame"
        # "dframe", id(self), "init", repr(filename)
        self.filename = filename
        # min and max key assertions (can be too far, truncated values)
        # if minKey is non-None and maxKey is None then "last buffer -- no maximum"
        self.minKey = self.maxKey = None
        # dictionary representation
        self.D = {}
        # keys list representation
        self.keys = None
        # values list representation
        self.values = None
        # marshal string representation (prefer to leave unexploded)
        self.marshalString = None
        # number of pairs
        self.size = None
        # estimated byte size
        self.byteSize = None
        #pr  "inited", repr(self)

    def isExploded(self):
        "return True if the data values are unmarshalled"
        return (self.D is not None) or (self.keys is not None)
    
    def __repr__(self, limit=True):
        "string representation for debugging"
        if self.filename:
            L = ["DataFrame(filename=%s," % repr(self.filename)]
        else:
            L = ["DataFrame( "]
        (k,v) = self.sortedKeysAndValues(noNull=False)
        for i in xrange(len(k)):
            if limit and len(L)>3:
                L.append("...")
                break
            L.append(repr( (k[i],v[i]) ) )
        L.append(")")
        L.append(" empty="+repr(self.empty()))
        return "".join(L)
    
    def empty(self, explode=True, emptyDataHeuristic=100):
        "return true if empty (don't unmarshal if explode=False)"
        #if self.byteSize==0:
        #    return True
        marshalString = self.marshalString
        keys = self.keys
        D = self.D
        size = self.size
        if not (keys or D or size) and marshalString:
            # "must explode to test empty"
            if explode:
                # "getting keys"
                (k,v) = self.sortedKeysAndValues(noNull=False)
                if k:
                    return False
                else:
                    return True
            else:
                # don't exploded it, assume it is empty if data size is smaller than heuristic
                if len(marshalString)<emptyDataHeuristic:
                    return True
                else:
                    return False
        if keys:
            return False
        if D:
            return False
        if size:
            return False
        return True
    
    def LeafGenerator(self, fromKey=None, atEnd=True):
        "generate sorted leaf sequence which may contain key (not at end if atend set false)"
        #pr "dframe.LeafGenerator", (fromKey, atEnd)
        # only check inrange if atEnd is false
        giveEmpty = self.empty(explode=True)
        if not atEnd and fromKey is not None and not giveEmpty:
            (keys, values) = self.sortedKeysAndValues()
            if fromKey>=keys[-1]:
                # fromKey is at end or out of range: give empty sequence as output
                giveEmpty = True
        if giveEmpty:
            #pr "dframe generating empty leaf sequence"
            #pr self.keys
            #pr fromKey
            iterator = iter( [None,] )
        else:
            #pr "dframe generating leaf followed by None"
            iterator = iter( [self, None] )
        return frameGenerators.SortedFrames( iterator )
        
    def dataSize(self):
        "estimate the byte size of data in self"
        # XXXX is this a performance problem?
        bs = self.byteSize
        if bs is not None:
            return bs
        bs = self.byteSize = len(self.asMarshal())
        return bs
    
    def setSize(self, size):
        "force frame to appear to have a certain size"
        self.size = size
        
    def getSize(self):
        "estimate the number of pairs in self"
        s = self.size
        if s is not None:
            return s
        D = self.D
        if D is not None:
            s = self.size = len(D)
            return s
        keys = self.keys
        if keys:
            s = self.size = len(keys)
            return s
        (k, v) = self.sortedKeysAndValues()
        s = self.size = len(k)
        return s
    
    def dump(self):
        "debug data dump"
        (k,v) = self.sortedKeysAndValues()
        print "  keys:", k
        print "  values:", v
        
    def setMinMax(self, minkey=None, maxkey=None):
        "force frame to appear to have given min and max key values (for key truncation)"
        #### DEBUG
        ##pr "setMinMax", (minkey, maxkey, self.minKey, self.maxKey)
        #if self.minKey>self.maxKey:
        #    #pr "keys", self.keys[0], self.keys[-1]
        #    raise ValueError, "bad initial values!"
        ### END DEBUG
        #pr "setMinMax initial values", repr(self.minKey)[:20], repr(self.maxKey)[:20]
        if minkey is not None:
            self.minKey = minkey
        if maxkey is not None:
            self.maxKey = maxkey
        orderBad = False
        try:
            orderBad = self.minKey is not None and self.maxKey is not None and self.minKey>self.maxKey
        except:
            pass # unicode conversion problem: ignore here (??)
        if orderBad:
            raise ValueError, "invalid setMinMax "+repr((minkey, maxkey, self.minKey, self.maxKey))
    
    def getMinMax(self, check=False):
        "report the (possibly overestimated) minimum and maximum key values"
        mn = self.minKey
        mx = self.maxKey
        #"getMinMax", (mn,mx,check)
        if mx is None or mn is None or check:
            # compute the min and max
            # XXXX should max be *beyond* the last value?
            (k, v) = self.sortedKeysAndValues()
            # "k,v", (k,v)
            if len(k)<1:
                raise ValueError, "no min/max: empty structure"
            mn = self.minKey = k[0]
            mx = self.maxKey = k[-1]
            self.minKey = mn
            self.maxKey = mx
        ###
        # DEBUG!
        #try:
        #    (ks, vs) = self.sortedKeysAndValues()
        #except:
        #    pass
        #else:
        #    if ks[0]<mn:
        #        raise ValueError, "INVALID MIN "+repr(mn, ks[0])
        #    if ks[-1]>mx:
        #        raise ValueError, "INVALID MAX "+repr(mx, ks[-1])
        ###
        # "returning", (mn,mx)
        return (mn, mx) # ????
    
    def loadData(self, fromFile, update=False, data=None):
        "load marshal string from file, exploding it automatically"
        # data if provided is already unmarshalled data
        #pr "loadData", (fromFile, update, data)
        if data is None:
            self.marshalString = None # could read back from file if that would be useful
            if fromFile is None:
                raise ValueError, "one of data or fromFile must be provided"
            # id(self), "unmarshalling from file", file.tell()
            startseek = fromFile.tell()
            data = marshal.load(fromFile)
            self.byteSize = fromFile.tell()-startseek
        self.minKey = self.maxKey = None # compute as needed
        #pr "loadData", data
        if type(data) is types.DictType:
            if update:
                D = self.asDictionary()
                D.update(data)
            else:
                self.D = data
            self.keys = self.values = None
        else:
            (k,v) = data
            if update:
                # load into dictionary
                # "UPDATING", k
                #pr "updating", len(k)
                D = self.asDictionary()
                for i in xrange(len(k)):
                    D[ k[i] ] = v[i]
                #pr "done updating"
                self.size = len(D)
                # "AFTER UPDATE", D
                self.keys = self.values = None
                self.marshalString = None
            else:
                self.keys = k
                self.values = v
                self.D = None
                self.size = len(k)
                if k:
                    self.minKey = k[0]
                    self.maxKey = k[-1]
                self.D = None
        #pr  "loaded", repr(self)
        
    def loadString(self, fromFile,
                   bytelength=None, minKey=None, maxKey=None, size=None, recordSeek=None, noData=False):
        "load marshal string from file without exploding it (if length is None load to eof) return byte size"
        #pr id(self), "loading from string minKey=", repr(minKey), "maxKey=", repr(maxKey), "byteLength=", bytelength
        #pr "file=", fromFile
        #pr "seek =", fromFile.tell()
        self._recordSeek = recordSeek
        if bytelength is None:
            #pr "now reading remainder", fromFile.tell()
            startSeek = fromFile.tell()
            self.marshalString = fromFile.read()
            #pr "read gets", self.marshalString
            endSeek = fromFile.tell()
            bytelength = endSeek-startSeek
        else:
            # "now reading length", bytelength, "from", fromFile.tell()
            if noData:
                # just skip the data (special case optimization for scanning files)
                self.marshalString = None
                endSeek = fromFile.tell()+bytelength
                fromFile.seek(endSeek)
            else:
                self.marshalString = fromFile.read(bytelength)
                endSeek = fromFile.tell()
        self.endSeek = endSeek
        #pr  "loadString", repr(self.marshalString)
        self.size = size
        if maxKey is not None:
            if minKey>maxKey:
                raise "bad extrema: "+repr((minKey, maxKey))
            self.minKey = minKey
            self.maxKey = maxKey
        # otherwise leave min and max key alone in case someone smarter set them already.
        self.D = self.keys = self.values = None
        #pr  "loaded", repr(self), bytelength
        self.byteSize = bytelength
        return bytelength

    def getRecordSeek(self):
        return self._recordSeek

    def setRecordSeek(self, seek):
        self._recordSeek = seek
    
    def store(self, toFile, recordSeek=None, check=True):
        "store marshal representation for self in toFile at current seek position"
        # short cut: if marshalString is available, just write it
        #pr "storing", self.getSize(), self.dataSize()
        self._recordSeek = recordSeek
        startSeek = toFile.tell()
        m = self.marshalString
        if m is not None:
            toFile.write(m)
            self.byteSize = b = toFile.tell()-startSeek
            return b
        # otherwise dump to file
        data = self.D
        keys = self.keys
        if keys is not None:
            if check:
                lastkey = None
                for k in keys:
                    if k<=lastkey:
                        raise ValueError, "bad key order "+repr((lastkey, k))
                    lastkey = k
            data = (keys, self.values)
        if data is not None:
            marshal.dump(data, toFile)
            self.byteSize = b = toFile.tell()-startSeek
            return b
        # unreachable? or error?
        m = self.asMarshal()
        toFile.write(m)
        return len(m)
        
    
    def finalize(self, toFileName=None):
        "store self to filename"
        #pr "dframe finalize", id(self), (self.filename, toFileName, self.writeable)
        #pr "self = ", self
        if toFileName is not None:
            self.filename = toFileName
            self.writeable = True
        if self.writeable:
            #pr "dframe finalizing to", self.filename
            f = file(self.filename, "wb")
            self.store(f)
            f.close()
        #pr  "finalized", repr(self)
        
    def readOpen(self, fromFileName=None, writeable=False, update=False):
        "read self from filename"
        self.writeable = writeable
        if fromFileName is None:
            fromFileName = self.filename
        #pr "before readOpen", (fromFileName, writeable, update)
        #pr "self is", self
        f = file(fromFileName, "rb")
        self.loadData(f, update=update)
        f.close()
        #pr "after readopen"
        #pr "self is", self
        # "DFRAME readopened", repr(self)
        
    def asDictionary(self, noNull=True):
        "get or derive dictionary representation for self"
        D = self.D
        k = self.keys
        if D is None and k is None:
            m = self.marshalString
            if m is None:
                if noNull:
                    raise ValueError, "null data frame "
                D = self.D = {}
                return D
            data = marshal.loads(m)
            self.loadData(None, data=data)
            D = self.D
        if D is not None:
            return D
        k = self.keys
        v = self.values
        if k is None:
            raise ValueError, "null data frame (2)"
        D = {}
        v = self.values
        for i in xrange(len(k)):
            D[k[i]] = v[i]
        #pr  "asdict", repr(self)
        self.D = D
        return D
    
    #COUNTER = [0] # for debug
    
    def sortedKeysAndValues(self, noNull=True):
        "get or derived sorted lists of (keys, values) for pairs in self"
        k = self.keys
        D = self.D
        #pr "sortedKeysAndValues", (noNull,)
        #pr "  k=", repr(k)
        #pr "  D=", repr(D)
        if k is None and D is None:
            m = self.marshalString
            #pr "  m=", repr(m)
            if m is None:
                if noNull:
                    raise ValueError, "null data frame"
                k = self.keys = []
                v = self.values = []
                return (k,v)
            # unmarshal
            data = marshal.loads(m)
            #pr "marshal data", repr(data)
            self.loadData(None, data=data)
            D = self.D
            k = self.keys
        if k is not None:
            return (k, self.values)
        D = self.D
        if D is not None:
            k = D.keys()
            k.sort()
            self.keys = k
            v = self.values = [ D[key] for key in k ]
            return (k,v)
        # this is unreachable?
        if noNull:
            raise ValueError, "null data frame "#+repr(self)
        k = self.keys = []
        v = self.values = []
        return (k,v)
    
    def asMarshal(self, noNull=True):
        "get or derive marshal representation for self"
        #pr "asMarshal", self
        m = self.marshalString
        if m is not None:
            #pr "marshal string cached!"
            return m
        k = self.keys
        v = self.values
        if k is None:
            D = self.D
            if D is None:
                if noNull:
                    raise ValueError, "null data frame"
                else:
                    # "SETTING NULL (2)", (id(self), self.keys, self.values, self.D, self.marshalString)
                    D = self.D = {}
            #pr "now marshalling", D
            m = self.marshalString = marshal.dumps( D )
            self.byteSize = len(m)
            return m
        # id(self), "now marshalling"
        #pr "now marshalling", repr((k,v))
        m = self.marshalString = marshal.dumps( (k,v) )
        self.byteSize = len(m)
        return m
    
    def close(self):
        "for consistency with dtree: do nothing"
        pass

    def create(self, filename=None):
        "make empty structure (for consistancy with dtree)"
        if filename is not None:
            self.filename = filename

    def putDictionary(self, dictionary):
        "bulk load dictionary contents."
        D = self.D
        if D is None:
            D = self.asDictionary()
        D.update(dictionary)
        self.byteSize = self.marshalString = self.minKey = self.maxKey = self.keys = self.values = self.size = None

    def putKeysAndValues(self, keys, values):
        "put sorted keys with matching values (sort not validated)"
        lenk = len(keys)
        if lenk<1:
            return # do nothing
        if len(keys)!=len(values):
            raise ValueError, "bad lengths"
        k = self.keys
        v = self.values
        if k is None:
            (k, v) = self.sortedKeysAndValues()
        if k:
            maxk = k[-1]
            minks = keys[0]
            if maxk>=minks:
                raise ValueError, "bad key insertion ordering "+repr((maxk, minks))
        # k is alias for self.keys...
        k.extend(keys)
        v.extend(values)
        self.byteSize = self.D = self.marshalString = self.minKey = self.maxKey = None
        
    def __setitem__(self, key, value): # aggregate
        "add key value association"
        # use putKeysAndValues instead whenever possible
        #raise ValueError, "deferred"
        D = self.asDictionary(noNull=False)
        D[key] = value
        self.byteSize = self.keys = self.values = self.marshalString = self.minKey = self.maxKey = None
        

    def has_key(self, item):
        "test if item exists as a key in the structure"
        D = self.asDictionary()
        return D.has_key(item)

    def __delitem__(self, item):
        "remove key value association"
        D = self.asDictionary()
        if D.has_key(item):
            if self.byteSize<0:
                self.byteSize = 0
            del D[item]
            self.byteSize = self.marshalData = self.minKey = self.maxKey = self.keys = self.values = self.size = None
                
    def firstKeyValue(self):
        "return (firstkey, firstvalue) or None if empty"
        k = self.keys
        v = self.values
        if k is None:
            (k, v) = self.sortedKeysAndValues()
        if k:
            return (k[0], v[0])
        return None
    
    def get(self, key, default=None):
        "return value associated with key"
        D = self.asDictionary()
        return D.get(key, default)
    
    def __getitem__(self, key):
        "v = self[k] implementation (alias for get)"
        D = self.asDictionary()
        return D[key]

    def atEnd(self, key):
        (k,v) = self.sortedKeysAndValues()
        return k[-1]<=key

    def inRange(self, key, atEnd=True):
        (k, v) = self.sortedKeysAndValues()
        if not k:
            return False
        if atEnd:
            return k[0]<=key and key<=k[-1]
        else:
            # return false if at end point
            return k[0]<=key and key<k[-1]
    
    def nextKeyValueAfter(self, key, bsect=bisect.bisect_right):
        "find (nextKey, value) for nextKey larger than key, or return None if absent"
        k = self.keys
        v = self.values
        if k is None:
            (k, v) = self.sortedKeysAndValues()
        index = bsect(k, key)
        if index>=len(k):
            return None
        return (k[index], v[index])
    
    def findAtOrNextKeyValue(self, key, forceNext=False):
        "find (key, value) for value associated with key or (nextkey, nextvalue) if forceNext or absent, or return None"
        if not forceNext:
            D = self.D
            # xxx don't force a dict conversion: not needed for important cases!
            #if D is None:
            #    D = self.asDictionary()
            if D is not None and D.has_key(key):
                # shortcut
                return (key, D[key])
        # XXXX optimization for sequential scans omitted
        # possibly inline
        if forceNext:
            searcher = bisect.bisect_right
        else:
            searcher = bisect.bisect_left
        return self.nextKeyValueAfter(key, bsect=searcher)

    def rangeLists(self, fromKey, toKey=None, excludeSmallest=False, excludeLargest=False):
        "find lists of (keys, values) for keys greater-eq than fromKey and smaller-eq toKey"
        k = self.keys
        v = self.values
        if k is None:
            (k,v) = self.sortedKeysAndValues()
        lkeys = len(k)
        if lkeys<1:
            #p "lkeys too small"
            return ([],[])
        startIndex = 0
        endIndex = lkeys
        if fromKey is not None:
            if excludeSmallest:
                startIndex = bisect.bisect_right(k, fromKey)
            else:
                startIndex = bisect.bisect_left(k, fromKey)
            #p "truncated fromKey", (startIndex, endIndex)
        if toKey is not None:
            if excludeLargest:
                endIndex = bisect.bisect_left(k, toKey)
            else:
                endIndex = bisect.bisect_right(k, toKey)
        result = ( k[startIndex:endIndex], v[startIndex:endIndex] )
        return result
    
    def indexOf(self, key):
        "return index of nearest key in structure to key"
        k = self.keys
        #v = self.values
        if k is None:
            (k, v) = self.sortedKeysAndValues()
        return bisect.bisect_left(k, key)
    
    def lastIndex(self):
        "estimate largest index"
        return self.getSize()


# testing stuff...

def test1(f = TESTDIR+"tree.dat"):
    import string
    print "running", test1
    d = DataFrame() 
    d.create(f)
    i = 0
    for a in list(string.lowercase):
        d[a] = i
        i+=1
    d.finalize()
    d = DataFrame()
    d.readOpen(f)
    i = 0
    for l in list(string.lowercase):
        j = d[l]
        if j!=i:
            raise ValueError, "bad retrieval "+repr((l,i,j))
        i+=1
    i = 0
    for l in list(string.lowercase):
        j = d.indexOf(l)
        if i!=j:
            raise ValueError, "bad indexOf "+repr((l,i,j))
        i+=1
    if d.lastIndex()!=len(string.lowercase):
        raise ValueError, "bad lastindex"
    (k,v) = d.rangeLists("c", "n")
    print k,v

if __name__=="__main__":
    test1()
    
