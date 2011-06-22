
"""
operations on generators including frame sequences (generators).

All generators are assumed to be None terminated (rather than exception terminated).
"""

# XXXX I think it is assumed that no frame is ever empty... (frame sequences can be empty tho)

import dframe
import marshal
import shuffle
import scatter
from nucular import parameters

# types for string truncation
from types import TupleType, StringType, UnicodeType, StringTypes

# constant for string key truncation
try:
    MAXUNICHR = unichr(0x10fffd)
except:
    MAXUNICHR = unichr(0xffff) #damn!

# constants for file format
PREFIXFORMAT = "%08d&%08d"
PREFIXLENGTH = len(PREFIXFORMAT % (123456,1222) )

class GeneratorWrapper:
    "superclass for wrapping generator objects to associate methods"
    # this is intended to make code a bit more readable and less type-error prone
    def __init__(self, source):
        "usage below assumes that source is a None terminated iterator/sequence"
        self.source = iter(source) # coerce to iterator
    def iter(self):
        return self.source
    def __iter__(self):
        return self.source

class Lists(GeneratorWrapper):
    def clone(self, source):
        return Lists(source)
    def GetItem(self, index):
        newsource = IndexLists(self.source, index)
        return Lists(newsource) # this won't preserve sorted ordering, generally! hence Lists
    def MakeTuples(self, prefix=None, suffix=None):
        newsource = TupleLists(self.source, prefix, suffix)
        return self.clone(newsource)
    def CombineToMinSize(self, minSize=100):
        newsource = CombineListsToMinSize(self.source, minSize)
        return self.clone(newsource)
    def Sort(self):
        newsource = sortLists(self.source)
        return SortedLists(newsource)

def sortLists(source):
    "collect elements from source, removing duplicates: combine into a (maybe large) trivial sorted list"
    D = {}
    for L in source:
        if L is None:
            break
        D.fromkeys(L)
    keys = D.keys()
    keys.sort()
    yield keys
    yield None # sentinel

def IndexLists(source, index):
    for L in source:
        if L is None:
            break
        emit = [ x[index] for x in L ]
        yield emit
    yield None # sentinel

def CombineListsToMinSize(source, minSize):
    thisList = source.next()
    while thisList is not None:
        if len(thisList)>=minSize:
            yield thisList
        else:
            collector = []
            size = 0
            while size<minSize and thisList is not None:
                collector.append(thisList)
                size += len(thisList)
                thisList = source.next()
            bigList = [None]*size
            nextIndex = 0
            for L in collector:
                endIndex = nextIndex + len(L)
                bigList[nextIndex:endIndex] = L
                nextIndex = endIndex
            if endIndex!=size:
                raise ValueError, "logic error, end not reached"
            yield bigList
    yield None # sentinel

class SortedLists(Lists):
    def Align(self, other):
        newsource = AlignSortedLists(self.source, other.source)
        return AlignedListPairs(newsource)
    def clone(self, source):
        return SortedLists(source)
    def __or__(self, other):
        "union operation for sorted lists"
        aligned = self.Align(other)
        return aligned.Union()
    def __and__(self, other):
        "intersection operation for sorted lists"
        aligned = self.Align(other)
        return aligned.Intersect()
    def __sub__(self, other):
        "difference operation for sorted lists"
        aligned = self.Align(other)
        return aligned.Difference()

def TupleLists(source, prefix=None, suffix=None):
    for L in source:
        if L is None:
            break
        emit = [ (x,) for x in L ]
        if prefix is not None:
            emit = [ prefix+e for e in emit ]
        if suffix is not None:
            emit = [ e+suffix for e in emit ]
        yield emit
    yield None # sentinel

def AlignSortedLists(sourceA, sourceB):
    """
    from two sorted list generators, generate pairs (Achunk, Bchunk)
    where the Achunks contain the same elements as sourceA and
    the Bchunks contain the same elements as sourceB and
    any pair contains only elements greater than all previous pairs and
    any pair contains only elements smaller than all following pairs and
    the algorithm attempts to make large chunks without expending too much effort.
    Assumption: the sources must be in sorted order and contain no duplicates.
    """
    from bisect import bisect_right
    currentA = sourceA.next()
    currentB = sourceB.next()
    currentAindex = currentBindex = 0
    while currentA is not None and currentB is not None:
        # skip past any empty/exhausted lists
        lenA = len(currentA)
        lenB = len(currentB)
        if currentAindex>=lenA:
            currentA = sourceA.next()
            currentAindex = 0
        elif currentBindex>=lenB:
            currentB = sourceB.next()
            currentBindex = 0
        else:
            # both are non-empty: use bisect to find matching subsections
            maxA = currentA[-1]
            maxB = currentB[-1]
            limit = min(maxA, maxB)
            nextAindex = bisect_right(currentA, limit)
            nextBindex = bisect_right(currentB, limit)
            currentAchunk = currentA
            if nextAindex<lenA or currentAindex>0:
                currentAchunk = currentA[currentAindex:nextAindex]
            currentBchunk = currentB
            if nextBindex<lenA or currentBindex>0:
                currentBchunk = currentB[currentBindex:nextBindex]
            if not currentAchunk and not currentBchunk:
                raise ValueError, "logic problem: one of the lists should not be empty"
            yield (currentAchunk, currentBchunk)
            currentAindex = nextAindex
            currentBindex = nextBindex
            if not currentAindex>=lenA and not currentBindex>=lenB:
                raise ValueError, "logic problem: one of the lists should be exhausted"
    # emit any remainders
    while currentA is not None:
        if currentAindex>0:
            currentAchunk = currentA[currentAindex:]
        else:
            currentAchunk = currentA
        if currentAchunk:
            yield (currentAchunk, None) # None marks end of rhs
        currentA = sourceA.next()
        currentAindex = 0
    while currentB is not None:
        if currentBindex>0:
            currentBchunk = currentB[currentBindex:]
        else:
            currentBchunk = currentB
        if currentBchunk:
            yield (None, currentBchunk) # None marks end of lhs
        currentB = sourceB.next()
        currentBindex = 0
    yield None # sentinel
    
class AlignedListPairs(GeneratorWrapper):
    """
    wrapper for pair generators satisfying the alignment property described in AlignedSortedLists.
    """
    def Union(self):
        "union each aligned pair"
        newsource = pairsUnionGenerator(self.source)
        return SortedLists(newsource)
    def Intersect(self):
        newsource = pairsIntersectionGenerator(self.source)
        return SortedLists(newsource)
    def Difference(self):
        newsource = pairsDifferenceGenerator(self.source)
        return SortedLists(newsource)

def pairsIntersectionGenerator(pairSource):
    "from an aligned pair sequence generate the intersection as a sorted list sequence"
    for pair in pairSource:
        if pair is None:
            break
        (SL1, SL2) = pair
        # break on either side end marker
        if SL1 is None or SL2 is None:
            break
        if SL1 and SL2:
            intersection = SLintersect(SL1, SL2)
            if intersection:
                yield intersection
    yield None # sentinel

def SLintersect(SL1, SL2):
    "sorted list intersect"
    # possibly should use the set module
    len1 = len(SL1)
    len2 = len(SL2)
    maxOutLen = min(len1, len2)
    OutBuffer = [None]*maxOutLen
    outCount = count1 = count2 = 0
    while count1<len1 and count2<len2:
        elt1 = SL1[count1]
        elt2 = SL2[count2]
        if elt1==elt2:
            OutBuffer[outCount] = elt1
            outCount += 1
            count1 += 1
            count2 += 1
        elif elt1<elt2:
            count1 += 1
        elif elt2<elt1: # could change to else after validation
            count2+= 1
        else:
            raise ValueError, "unreachable code"
    return OutBuffer[:outCount]

def pairsUnionGenerator(pairSource):
    "from an aligned pair sequence generate the union as a sorted list sequence"
    for pair in pairSource:
        if pair is None:
            break
        (SL1, SL2) = pair
        if SL1 and SL2:
            union = SLunion(SL1, SL2)
            yield union
        elif SL1:
            yield SL1
        elif SL2:
            yield SL2
    yield None # sentinel

def SLunion(SL1, SL2):
    "sorted list union"
    # possibly should use the set module
    len1 = len(SL1)
    len2 = len(SL2)
    maxOutLen = len1+len2
    OutBuffer = [None]*maxOutLen
    outCount = count1 = count2 = 0
    while count1<len1 and count2<len2:
        elt1 = SL1[count1]
        elt2 = SL2[count2]
        if elt1==elt2:
            OutBuffer[outCount] = elt1
            outCount += 1
            count1 += 1
            count2 += 1
        elif elt1<elt2:
            OutBuffer[outCount] = elt1
            outCount += 1
            count1 += 1
        elif elt2<elt1: # could change to else after validation
            OutBuffer[outCount] = elt2
            outCount += 1
            count2+= 1
        else:
            raise ValueError, "unreachable code"
    return OutBuffer[:outCount]

def pairsDifferenceGenerator(pairSource):
    "from an aligned pair sequence generate the union as a sorted list sequence"
    for pair in pairSource:
        if pair is None:
            break
        (SL1, SL2) = pair
        if SL1 and SL2:
            difference = SLdifference(SL1, SL2)
            if difference:
                yield difference
        elif SL1:
            yield SL1
        # look for lhs end marker
        if SL1 is None:
            break
    yield None # sentinel

def SLdifference(SL1, SL2):
    "sorted list difference"
    # possibly should use the set module
    len1 = len(SL1)
    len2 = len(SL2)
    maxOutLen = len1
    OutBuffer = [None]*maxOutLen
    outCount = count1 = count2 = 0
    while count1<len1 and count2<len2:
        elt1 = SL1[count1]
        elt2 = SL2[count2]
        if elt1==elt2:
            count1 += 1
            count2 += 1
        elif elt1<elt2:
            OutBuffer[outCount] = elt1
            outCount += 1
            count1 += 1
        elif elt2<elt1: # could change to else after validation
            count2+= 1
        else:
            raise ValueError, "unreachable code"
    return OutBuffer[:outCount]

class Frames(GeneratorWrapper):
    "wraps a generator for DataFrames which may not be sorted"
    def clone(self, source):
        return Frames(source)
    def JoinTooSmallbySize(self, tooSmallSize, label="X"):
        newsource = joinTooSmallSortedFramesBySize(self.source, tooSmallSize, label)
        return self.clone(newsource)
    def SplitTooLargeFrames(self, tooLargeDataSize):
        newsource = splitTooLargeFrames(self.source, tooLargeDataSize)
        return self.clone(newsource)
    def JoinTooSmallByBytes(self, tooSmallDataSize):
        newsource = joinTooSmallSortedFramesByBytes(self.source, tooSmallDataSize)
        return self.clone(newsource)
    def AdjustDataSizes(self, tooLargeDataSize, tooSmallDataSize):
        newsource = adjustFrameDataSizes(self.source, tooLargeDataSize, tooSmallDataSize)
        return self.clone(newsource)
    def ToFileWithStats(self, toFile, atEnd=True):
        #pr "ToFileWithStats", toFile
        return storeFramesToFileWithStats(self.source, toFile, atEnd)
    def ScatterSorterAnalyse(self, scratchFile, youngerToRight=True):
        # note: scatter sorting will require two copies of self.source to complete
        SSorter = scatter.ScatterSorter(scratchFile, youngerToRight)
        # load the sorter
        for frame in self.source:
            if frame is None:
                break
            D = frame.asDictionary()
            SSorter.analyseDictionary(D)
        SSorter.setUpSampling()
        return SSorter
    def ScatterSortAfterAnalysis(self, SampledScatterSorter, tooSmallDataSize):
        # here self.source should be a fresh copy of the generator just analysed
        for frame in self.source:
            if frame is None:
                break
            D = frame.asDictionary()
            SampledScatterSorter.scatterDict(D)
        pairsSource = SampledScatterSorter.pairsGenerator()
        #source = SSorter.LeafGenerator(emitDataSize)
        source = getFramesFromPairsGenerator(pairsSource, tooSmallDataSize, sorted=True)
        return SortedFrames(source)
    def SimpleSort(self):
        "in memory simple sort, on ambiguity prefer later values"
        allD = {}
        for frame in self.source:
            if frame is None:
                break
            D = frame.asDictionary()
            #pr "simplesort adding", D
            allD.update(D)
        keys = allD.keys()
        keys.sort()
        values = [ allD[k] for k in keys ]
        #frame = dframe.DataFrameFromLists(keys, values)
        frames = splitFrameGenerator(keys, values, parameters.LTreeNodeSize)
        return SortedFrames( frames )
        #pairsSource = iter( [ (keys, values), None ] )
        #source = getFramesFromPairsGenerator(pairsSource, tooSmallDataSize=None, sorted=True)
        #return SortedFrames(source)
    def OnlyFrame(self):
        result = self.source.next()
        after = self.source.next()
        if after is not None:
            raise ValueError, "more than one frame found in source"
        return result
    def RemoveNoneValuesIfExploded(self, fromKey, atEnd):
        source = RemoveNoneValuesIfExploded(self.source, fromKey, atEnd)
        return self.clone(source)

def RemoveNoneValuesIfExploded(source, fromKey, atEnd):
    """
    Pass over a frame generator.
    Replace frames that are exploded that have None values with
    frames with the None entries removed.
    [special operation for cleaning shadowed structures of delete marks.]
    """
    for f in source:
        if f is None:
            break
        if f.isExploded():
            # assume keys/values are more accessible than dict usually
            (keys, values) = f.sortedKeysAndValues()
            if None in values:
                #pr "remove nulls detected nulls in frame"
                D = {}
                for i in xrange(len(keys)):
                    k = keys[i]
                    v = values[i]
                    if v is not None:
                        D[k] = v
                newframe = dframe.DataFrame()
                if D and not atEnd:
                    # ignore this frame if fromKey lies at or beyond the last key
                    maxKey = max(D)
                    #pr "maxKey, fromKey", maxKey, fromKey
                    if fromKey>=maxKey:
                        #pr "remove nulls skipping fromkey too big"
                        D = None # skip it.
                if D:
                    #pr "remove nulls yielding frame", len(D)
                    newframe.putDictionary(D)
                    yield newframe
            else:
                yield f
        else:
            yield f
    yield None # sentinel

# this should be a Frames class method
def FramesFromDFrameFilePaths(filePaths, sorted=False):
    source = FramesGeneratorFromDFrameFilePaths(filePaths)
    if sorted:
        return SortedFrames(source)
    else:
        return Frames(source)

def FramesFromPairs(pairs, sizeLimit=20):
    source = FramesGeneratorFromPairs(pairs, sizeLimit)
    return Frames(source)

def FramesGeneratorFromPairs(pairs, sizeLimit=20):
    "mostly for testing"
    #pr "frames from pairs", pairs
    keys = []
    values = []
    for p in pairs:
        if p is None:
            break
        (k,v) = p
        keys.append(k)
        values.append(v)
        if len(k)>=sizeLimit:
            #pr "dumping", keys, values
            frame = dframe.DataFrameFromLists(keys, values, check=False)
            yield frame
            keys = []
            values = []
    if keys:
        frame = dframe.DataFrameFromLists(keys, values, check=False)
        #pr "dumping", keys, values
        yield frame
    #pr "end of frames from pairs"
    yield None # sentinel

class SortedFrames(Frames):

    ### for DB only!
    #def __init__(self, source):
    #    self.source = checkSortedFrames(source)
    ### end of DB
        
    def clone(self, source):
        return SortedFrames(source)
    def MakeInteriorNodes(self, sizeLimit, verbose=False):
        newsource = MakeInteriorNodes(self.source, sizeLimit, verbose=verbose)
        return self.clone(newsource)
    def Merge(self, other, preferLeft=True, label="X", verbose=False):
        newsource = MergeSortedFrames(self.source, other.source, preferLeft, label, verbose=verbose)
        return self.clone(newsource)
    def MultiMerge(self, others, mergeSize=100, label="."):
        frameWrappers = [self,]+list(others)
        return FramesMultiMerge(frameWrappers, mergeSize, label)
    def Range(self, smallestKey=None, largestKey=None, excludeSmallest=False, excludeLargest=False):
        newsource = SortedFrameRange(self.source, smallestKey, largestKey, excludeSmallest, excludeLargest)
        return self.clone(newsource)
    def IntersectingRange(self, smallestKey=None, largestKey=None, excludeSmallest=False, excludeLargest=False):
        newsource = SortedFrameSubsequenceIntersectingRange(self.source,
            smallestKey, largestKey, excludeSmallest, excludeLargest)
        return self.clone(newsource)
    def TruncateKeyStats(self):
        newsource = truncateKeyStatsInSortedFrames(self.source)
        return self.clone(newsource)

### for debugging
# def checkFrameOrder(f1, f2):
#     if f1 and f2:
#         print "check frame Order"
#         print "   ", f1.getMinMax()
#         print "   ", f2.getMinMax()
#     check = checkSortedFrames( iter( [f1, f2, None] ) )
#     test = list(check)
#     return f2

# def checkSortedFrames(source):
#     lastkey = lastmax = None
#     try:
#         framecount = 0
#         for frame in source:
#             if frame is None:
#                 break
#             (mn, mx) = frame.getMinMax()
#             if mn>mx:
#                 raise ValueError, "bad min/max "+repr((mn,mx))
#             try:
#                 (keys, values) = frame.sortedKeysAndValues()
#             except:
#                 pass # unmaterialized frame is ok
#             else:
#                 if keys[0]<mn:
#                     raise ValueError, "bad reported min "+repr((mn,keys[0]))
#                 if keys[-1]>mx:
#                     raise ValueError, "bad reported max "+repr((keys[-1],mx))
#                 keycount = 0
#                 for k in keys:
#                     if lastkey is not None and k<=lastkey:
#                         raise ValueError, "key order violation "+repr(
#                             (lastkey, k, framecount, keycount))
#                     lastkey = k
#                     keycount += 1
#                 if len(keys)!=len(values):
#                     raise ValueError, "bad key/value lengths "+repr((len(keys), len(values)))
#             yield frame
#             framecount += 1
#         yield None # sentinel
#     except:
#         ##pr "source generation test failed for", source
#         raise
### end for debugging

# these should be a SortedFrames class methods
def FramesMultiMerge(frameWrappers, mergeSize=100, label="."):
    sources = [ f.iter() for f in frameWrappers ]
    mergeSource = MultiMergeSortedFrames(sources, mergeSize, label)
    return SortedFrames(mergeSource)

def FramesFromFileWithStats(fromFile, startSeek=None, cache=None, sorted=True, noData=False):
    source = restoreFramesFromFileWithStats(fromFile, startSeek, cache, noData)
    if sorted:
        return SortedFrames(source)
    else:
        return Frames(source)

def SortedFramesFromDFrameFilePaths(filePaths, scratchFile, tooSmallDataSize, youngerToRight=True):
    # need to make two copies of source generator
    frames1 = FramesFromDFrameFilePaths(filePaths)
    frames2 = FramesFromDFrameFilePaths(filePaths)
    return SortedFramesFromDFrames(frames1, frames2, scratchFile, tooSmallDataSize, youngerToRight)

def SortedFramesFromDFrames(frames1, frames2, scratchFile, tooSmallDataSize, youngerToRight=True):
    "frames1 and frames2 should generate the same data items"
    # first analyse the frames
    scatterSorter = frames1.ScatterSorterAnalyse(scratchFile, youngerToRight)
    # then scatter sort them (using a fresh generator)
    return frames2.ScatterSortAfterAnalysis(scatterSorter, tooSmallDataSize)

# index generation

def MakeInteriorNodes(SortedFrames, sizeLimit, verbose=False):
    "from a sequence of disk resident frames, make a sequence of index nodes (at same level in tree)"
    currentNode = dframe.DataFrame()
    currentDataSize = 0
    count = 0
    childcount = 0
    for childNode in SortedFrames:
        count+=1
        if childNode is None:
            break # end of nodes
        if childNode.getSize()>0:
            (minKey, maxKey) = childNode.getMinMax()
            seek = childNode.getRecordSeek()
            if seek is None:
                raise ValueError, "record seek not set in child node, cannot index"
            currentNode[maxKey] = seek
            childcount += 1
            currentDataSize += len(marshal.dumps(maxKey)) + 8
            if currentDataSize>sizeLimit and childcount>1:
                if verbose:
                    print "made index node", (count, currentDataSize)
                yield currentNode
                currentNode = dframe.DataFrame()
                currentDataSize = 0
                childcount = 0
    if currentNode.getSize()>0:
        if verbose:
            print "made final index node at level", (count, currentDataSize)
        yield currentNode
    yield None # sentinel

# two way merge

# for debug
#mergecounter = [0]

def MergeSortedFrames(SortedFramesA, SortedFramesB, preferLeft=True, label="X", verbose=False):
    "merge two sorted frame sequences: on ambiguity prefer left hand values (if set)"
    #mergecounter[0] += 1
    #label = label+repr(mergecounter[0])
    # this version attempts to avoid "slicing large tails"
    frameA = SortedFramesA.next()
    frameB = SortedFramesB.next()
    Akeys = Avalues = Aindex = Bkeys = Bvalues = Bindex = Amax = Amin = Bmax = Bmin = None
    #lastframe = None # for DB
    if verbose:
        print label, "MERGESORTEDFRAMES"
    #pr "first A", frameA
    #pr "first B", frameB
    while frameA is not None and frameB is not None:
        # combine frame sequences
        #pr "comparing", frameA
        #pr "         ", frameB
        if Amin is None:
            (Amin, Amax) = frameA.getMinMax()
        if Bmin is None:
            (Bmin, Bmax) = frameB.getMinMax()
        #pr label, "A min/max", (Amin, Amax), id(frameA), frameA.getSize()
        #pr label, "B min/max", (Bmin, Bmax), id(frameB), frameB.getSize()
        # optimized cases: not intersections
        if Bmax<Amin:
            # emit B
            if Bkeys is not None and Bindex>0:
                if verbose:
                    print label, "emit B remainder"
                BkeysRemainder = Bkeys[Bindex:]
                BvaluesRemainder = Bvalues[Bindex:]
                newframe = dframe.DataFrameFromLists(BkeysRemainder, BvaluesRemainder)
                #lastframe = checkFrameOrder(lastframe, newframe) # DB
                yield newframe
            else:
                if verbose:
                    print label, "emit all of B"#, frameB.getMinMax(), frameA.getMinMax()
                #lastframe = checkFrameOrder(lastframe, frameB) # DB
                yield frameB
            # get next B
            frameB = SortedFramesB.next()
            Bkeys = Bvalues = Bindex = Bmin = Bmax = None # unexploded ordinance
        elif Amax<Bmin:
            # emit A
            if Akeys is not None and Aindex>0:
                if verbose:
                    print label, "emit A remainder"
                AkeysRemainder = Akeys[Aindex:]
                AvaluesRemainder = Avalues[Aindex:]
                newframe = dframe.DataFrameFromLists(AkeysRemainder, AvaluesRemainder)
                #lastframe = checkFrameOrder(lastframe, newframe) # DB
                yield newframe
            else:
                if verbose:
                    print label, "emit all of A"
                #lastframe = checkFrameOrder(lastframe, frameA) # DB
                yield frameA
            frameA = SortedFramesA.next()
            Akeys = Avalues = Aindex = Amin = Amax = None # unexploded ordinance
        else:
            # apparent intersection:
            # explode containers if not exploded
            if Akeys is None:
                (Akeys, Avalues) = frameA.sortedKeysAndValues()
                Aindex = 0
                Asize = len(Akeys)
                Amin = Akeys[0]
                Amax = Akeys[-1]
            if Bkeys is None:
                (Bkeys, Bvalues) = frameB.sortedKeysAndValues()
                Bindex = 0
                Bsize = len(Bkeys)
                Bmin = Bkeys[0]
                Bmax = Bkeys[-1]
            (mkeys, mvalues, Aindex, Bindex) = shuffle.shuffleMergeIndices(
                Akeys, Avalues, Aindex, Bkeys, Bvalues, Bindex, preferLeft)
            if not mkeys:
                raise ValueError, "logic problem: merge returned empty result"
            #pr label, "emit merged frame"
            newframe = dframe.DataFrameFromLists(mkeys, mvalues)
            #pr "new frame is", newframe
            #lastframe = checkFrameOrder(lastframe, newframe) # DB
            yield newframe
            # advance the exhausted frame(s)
            exhausted = False
            if Aindex>=Asize:
                if verbose:
                    print label, "frame intersection: A exhausted"
                frameA = SortedFramesA.next()
                Akeys = Avalues = Aindex = None # unexploded ordinance
                exhausted = True
                Amin = Amax = None
            else:
                Amin = Akeys[Aindex]
            if Bindex>=Bsize:
                if verbose:
                    print label, "frame intersection: B exhausted"
                frameB = SortedFramesB.next()
                Bkeys = Bvalues = Bindex = None # unexploded ordinance
                exhausted = True
                Bmin = Bmax = None
            else:
                Bmin = Bkeys[Bindex]
            if not exhausted:
                raise ValueError, "logic problem: shuffle should exhaust one of the frames"
    # emit any remainder
    if Bkeys is not None:
        # emit B remainder
        if Bindex>0:
            if verbose:
                print label, "emit B tail remainder"
            BkeysRemainder = Bkeys[Bindex:]
            BvaluesRemainder = Bvalues[Bindex:]
            newframe = dframe.DataFrameFromLists(BkeysRemainder, BvaluesRemainder)
            #lastframe = checkFrameOrder(lastframe, newframe) # DB
            yield newframe
        else:
            if verbose:
                print label, "emit B tail"
            #lastframe = checkFrameOrder(lastframe, frameB) # DB
            yield frameB
        frameB = SortedFramesB.next()
    if Akeys is not None:
        # emit A remainder
        if Aindex>0:
            AkeysRemainder = Akeys[Aindex:]
            AvaluesRemainder = Avalues[Aindex:]
            newframe = dframe.DataFrameFromLists(AkeysRemainder, AvaluesRemainder)
            if verbose:
                print label, "emit A tail remainder"
            #lastframe = checkFrameOrder(lastframe, newframe) # DB
            yield newframe
        else:
            if verbose:
                print label, "emit A tail"
            #lastframe = checkFrameOrder(lastframe, frameA) # DB
            yield frameA
        frameA = SortedFramesA.next()
    # now emit all remaining frames
    while frameA is not None:
        if verbose:
            print label, "dumping remaining frameA", 
        #lastframe = checkFrameOrder(lastframe, frameA) # DB
        yield frameA
        frameA = SortedFramesA.next()
    while frameB is not None:
        if verbose:
            print label, "dumping remaining frameB"
        #lastframe = checkFrameOrder(lastframe, frameB) # DB
        yield frameB
        frameB = SortedFramesB.next()
    #pr label, "merge complete sentinel"
    yield None # sentinel

# historical version: takes too many list tails when buffers are large
def MergeSortedFrames0(SortedFramesA, SortedFramesB, preferLeft=True, label="X"):
    "merge two sorted frame sequences: on ambiguity prefer left hand values (if set)"
    # frames should contain no dups
    frameA = SortedFramesA.next()
    frameB = SortedFramesB.next()
    while frameA is not None and frameB is not None:
        # combine the frame sequences
        (minA, maxA) = frameA.getMinMax()
        (minB, maxB) = frameB.getMinMax()
        #pr 
        #pr label, "MergeSortedFrames comparing", (maxA, maxB)
        # optimized cases -- no intersections
        if maxB<minA:
            #pr label, "merge yield all of frameB"
            yield frameB
            frameB = SortedFramesB.next()
        elif maxA<minB:
            #pr label, "yield all of frameA"
            yield frameA
            frameA = SortedFramesA.next()
        else:
            #pr label, " ...apparent intersection"
            # there is an apparent intersection, get the real (non-truncated) min/maxen
            (minA, maxA) = frameA.getMinMax(check=True)
            (minB, maxB) = frameB.getMinMax(check=True)
            #pr label, "  ... real maxen are", (maxA, maxB)
            # for symmetry swap so that the A side has the smallest largest key
            if maxA>maxB:
                #pr "   swapping"
                (SortedFramesA, frameA, minA, maxA,
                 SortedFramesB, frameB, minB, maxB) = (
                    SortedFramesB, frameB, minB, maxB,
                    SortedFramesA, frameA, minA, maxA)
                preferLeft = not preferLeft
            (keysA, valuesA) = frameA.sortedKeysAndValues()
            (keysB, valuesB) = frameB.sortedKeysAndValues()
            #pr label, "shuffle"
            (mkeys, mvalues, keysA, valuesA, keysB, valuesB) = shuffle.shuffleMerge(
                keysA, valuesA, keysB, valuesB, preferLeft)
            #pr label, "shuffle complete"
            if keysA:
                raise ValueError, "logic problem: keysA should always be exhausted after shuffle"
            if mkeys:
                #pr label, "yielding new frame", len(mkeys)
                newFrame = dframe.DataFrameFromLists(mkeys, mvalues)
                #pr newFrame.getMinMax()
                yield newFrame
            # advance frames
            if keysB:
                # store remainder of frameB in new frame
                #pr " making new frameB", len(keysB)
                frameB = dframe.DataFrameFromLists(keysB, valuesB)
            else:
                # frameB exhausted also, get next
                #pr label, " frameB exhausted"
                frameB = SortedFramesB.next()
                #pr label, "got next frameB"
            # frameA is always exhausted here, get next
            #pr label, "advancing frameA"
            frameA = SortedFramesA.next()
            #pr label, "got next frameA"
    # now emit all remaining frames
    while frameA is not None:
        #pr label, "dumping remaining frameA"
        yield frameA
        frameA = SortedFramesA.next()
    while frameB is not None:
        #pr label, "dumping remaining frameB"
        yield frameB
        frameB = SortedFramesB.next()
    #pr "merge complete sentinel"
    yield None # sentinel

# N way merge

def MultiMergeSortedFrames(SortedFrameGenerators, mergeSize=100, label="."):
    "merge many sorted frames generators into one"
    # basically build a balanced tree using 2 way merges
    # mergesize attempts to keep intermediate frames from getting too small
    ngenerators = len(SortedFrameGenerators)
    if ngenerators<1:
        #pr label, "multi empty case"
        return iter( [None,] )
    if ngenerators==1:
        #pr label, "multi single generator case"
        return SortedFrameGenerators[0]
    # otherwise recursive case: break the iterables in two and combine
    midpoint = int(ngenerators/2)
    leftGenerators = SortedFrameGenerators[:midpoint]
    rightGenerators = SortedFrameGenerators[midpoint:]
    #pr label, "multi splitting", len(leftGenerators), len(rightGenerators), ngenerators
    leftMerge = MultiMergeSortedFrames(leftGenerators, mergeSize, label+"L")
    rightMerge = MultiMergeSortedFrames(rightGenerators, mergeSize, label+"R")
    shuffle = MergeSortedFrames(leftMerge, rightMerge, label=label)
    #return shuffle
    return joinTooSmallSortedFramesBySize(shuffle, mergeSize, label)
    
# sub-sequence selection

def SortedFrameSubsequenceIntersectingRange(source, smallestKey, largestKey, excludeSmallest, excludeLargest):
    "this variant doesn't change the frames: just makes sure they have some elements in range"
    for f in source:
        if f is None:
            break # end of frames sentinel
        (mn, mx) = f.getMinMax()
        if largestKey is not None and largestKey<mn:
            # the minimum value is past the range: we are done
            break
        elif smallestKey is not None and smallestKey>mx:
            # before the beginning of the range: skip this frame and keep looking
            pass
        else:
            if smallestKey is None or smallestKey<mn:
                if largestKey is None or largestKey>mx:
                    # all of frame is in range
                    yield f # optimization: f is possibly unexploded!
                    continue # next frame please
            # otherwise check the real (not estimated) min/max
            (mn, mx) = f.getMinMax(check=True)
            # yield all of f if there is any intersection in range
            if smallestKey is not None and (mx>smallestKey or (excludeSmallest and mx==smallestKey)):
                continue # max value in frame is too small: skip this frame and keep looking
            if largestKey is not None and (mn<largestKey or (excludeLargest and mn==smallestKey)):
                break # mn is too big: this and all subsequent frames are out of range
            # otherwise this frame has an intersection
            yield f
    yield None # sentinal

def SortedFrameRange(SortedFrames, smallestKey=None, largestKey=None, excludeSmallest=False, excludeLargest=False):
    "provide subrange of frames with only keys in range"
    # skip frames up to frames in range
    frame = SortedFrames.next()
    if frame is not None and smallestKey is not None:
        (minKey, maxKey) = frame.getMinMax()
        while 1:
            if maxKey>=smallestKey:
                # check that it's not truncated
                (minKey, maxKey) = frame.getMinMax(check=True)
                if maxKey>=smallestKey:
                    break
            # maxKey is smaller than smallestKey: throw away the frame and move on
            frame = SortedFrames.next()
            if frame is None:
                break
            (minKey, maxKey) = frame.getMinMax()
    # valid frame has maxKey<=maxKey
    if frame is not None:
        done = False
        while not done:
            (minKey, maxKey) = frame.getMinMax() # don't explode it! accept over-estimate
            if largestKey is not None and (minKey>largestKey or minKey==largestKey and excludeLargest):
                # minKey>largestKey -- all remaining frames are out of range: we are done
                done = True
            else:
                # frame intersects range
                if (smallestKey is None or minKey>smallestKey) and (
                    largestKey is None or maxKey<largestKey):
                    # frame is subset of range: pass on the frame unchanged (possibly unexploded in optimal case)
                    yield frame
                else:
                    # frame appears to be overlapping range
                    # check if largest key is exceeded
                    (minKey, maxKey) = frame.getMinMax(check=True)
                    if largestKey is not None and maxKey>=largestKey:
                        done = True
                    # generate subframe
                    (keys, values) = frame.rangeLists(smallestKey, largestKey, excludeSmallest, excludeLargest)
                    if keys:
                        newFrame = dframe.DataFrameFromLists(keys, values)
                    yield newFrame
            if not done:
                # advance to next frame
                frame = SortedFrames.next()
                if frame is None:
                    done = True # last frame
    yield None # sentinel

# cardinality control utilities

def joinTooSmallSortedFramesBySize(FrameGenerator, tooSmallSize, label="X", sorted=True):
    done = False
    frame = FrameGenerator.next()
    while not done:
        #pr label, "too small getting new frame"
        if frame is None:
            #pr label, "too small at end of frames"
            break
        size = frame.getSize()
        if size>=tooSmallSize:
            #pr label, "too small size ok"
            yield frame
            frame = FrameGenerator.next()
        else:
            # combine this frame with others until it is big enough
            #pr label, "too small combining frames"
            listOfPairs = []
            keyCount = 0
            while keyCount<tooSmallSize and not done:
                pair = frame.sortedKeysAndValues()
                (keys, values) = pair
                listOfPairs.append(pair)
                #pr label, "too small appending", len(keys)
                keyCount += len(keys)
                frame = FrameGenerator.next()
                if frame is None:
                    done = True
            newframe = getFrameFromListOfPairs(listOfPairs, keyCount, sorted=True)
            if newframe is not None:
                #pr label, "too small yielding", keyCount
                yield newframe
    yield None # sentinel

# byte size control utilities (somewhat heuristic)

def splitTooLargeFrames(FrameGenerator, tooLargeDataSize):
    for frame in FrameGenerator:
        if frame is None:
            break
        if frame.dataSize()<=tooLargeDataSize:
            yield frame
        else:
            (keys, values) = frame.sortedKeysAndValues()
            for frame in splitFrameGenerator(keys, values, tooLargeDataSize):
                if frame is None:
                    break
                yield frame
            #nkeys = len(keys)
            #cursor = 0
            #while cursor<nkeys:
                #(nextCursor,size) = splitIndex(keys, values, tooLargeDataSize, cursor)
                #if nextCursor<=cursor:
                #    raise ValueError, "next cursor not advancing in frame split "+repr((
                #        nextCursor, cursor))
                #subkeys = keys[cursor:nextCursor]
                #subvalues = values[cursor:nextCursor]
                #newframe = dframe.DataFrameFromLists(subkeys, subvalues)
                #newframe.setSize(size)
                #yield newframe
                #cursor = nextCursor
    yield None # sentinel

def joinTooSmallSortedFramesByBytes(FrameGenerator, tooSmallDataSize, sorted=True):
    done = False
    frame = FrameGenerator.next()
    while not done:
        #frame = FrameGenerator.next()
        if frame is None:
            break # finished
        #pr "joiner looking at", frame.getMinMax()
        datasize = frame.dataSize()
        if datasize>=tooSmallDataSize:
            # frame is large enough: let it pass
            #pr "joiner passing on"
            yield frame
            frame = FrameGenerator.next()
        else:
            # keep combining this frame with next until large enough
            #pr "joiner combining"
            listOfPairs = []
            keyCount = 0
            datasize = 0
            while not done and datasize<tooSmallDataSize:
                #pr "   ...adding", frame.getMinMax()
                pair = frame.sortedKeysAndValues()
                (keys, values) = pair
                listOfPairs.append(pair)
                keyCount += len(keys)
                datasize += frame.dataSize()
                frame = FrameGenerator.next()
                if frame is None:
                    done = True
                    #break
            #pr "joiner constructing newframe", keyCount, datasize
            newframe = getFrameFromListOfPairs(listOfPairs, keyCount, datasize, sorted=True)
            if newframe is not None:
                #pr "joiner emitting newframe", newframe.dataSize()
                yield newframe
    yield None # sentinel

def getFramesFromPairsGenerator(PairsGenerator, tooSmallDataSize, chunkSize=20, sorted=True):
    "generate frames from pairs, larger (but not much larger) than tooSmallDataSize"
    #from marshal import dumps
    dumps = marshal.dumps
    listOfPairs = []
    keyCount = 0
    size = 0
    for pair in PairsGenerator:
        #pr "framing", pair
        if pair is None:
            break
        (keys, values) = pair
        index = 0
        nkeys = len(keys)
        while index<nkeys:
            nextIndex = index+chunkSize
            keyschunk = keys[index:nextIndex]
            valueschunk = values[index:nextIndex]
            index = nextIndex
            listOfPairs.append( (keyschunk, valueschunk) )
            keyCount += len(keyschunk)
            if tooSmallDataSize is not None:
                for k in keyschunk:
                    size += len(dumps(k))
                for v in valueschunk:
                    size += len(dumps(v))
            if tooSmallDataSize is None or size>tooSmallDataSize:
                frame = getFrameFromListOfPairs(listOfPairs, keyCount, size, sorted=sorted)
                if frame is None:
                    raise ValueError, "got null frame on size exceeded: shouldn't happen"
                #pr "yielding", frame
                yield frame
                listOfPairs = []
                keyCount = 0
                size = 0
    # yield any remainder
    if listOfPairs:
        frame = getFrameFromListOfPairs(listOfPairs, keyCount, size, sorted=sorted)
        #pr "yielding final frame from pairs", frame
        if frame is not None:
            yield frame
    yield None # sentinel

def getPairFromListOfPairs(listOfPairs, keyCount):
    allkeys = [None]*keyCount
    allvalues = [None]*keyCount
    cursor = 0
    for (keys, values) in listOfPairs:
        nkeys = len(keys)
        nextcursor = cursor+nkeys
        allkeys[cursor: nextcursor] = keys
        allvalues[cursor: nextcursor] = values
        cursor = nextcursor
    if cursor!=keyCount:
        raise ValueError, "logic problem: lists not initialized correctly "+repr(
            (cursor, keyCount))
    return (allkeys, allvalues)

def getFrameFromListOfPairs(listOfPairs, keyCount, dataSize=None, sorted=True):
    # allocate lists all at once because they might be big
    #pr "making frame from pairs list", len(listOfPairs), keyCount
    (allkeys, allvalues) = getPairFromListOfPairs(listOfPairs, keyCount)
    if allkeys:
        newframe = dframe.DataFrameFromLists(allkeys, allvalues, sorted)
        #pr "from pairs returning new frame", len(allkeys)
        if dataSize is not None:
            newframe.byteSize = dataSize # XXXX this should be arbitrated by a setter!
        return newframe
    return None # sentinel

def adjustFrameDataSizes(FrameGenerator, tooLargeDataSize, tooSmallDataSize):
    splitter = splitTooLargeFrames(FrameGenerator, tooLargeDataSize)
    #return splitter # for testing
    joiner = joinTooSmallSortedFramesByBytes(splitter, tooSmallDataSize)
    return joiner

def splitIndex(keys, values, targetSize, startIndex=0):
    #from marshal import dumps
    dumps = marshal.dumps
    size = 0
    nkeys = len(keys)
    for i in xrange(startIndex, len(keys)):
        k = keys[i]
        v = values[i]
        size += len(dumps(k))+len(dumps(v))
        if size>targetSize:
            return (i+1, size) # rhs slice boundary
    return (nkeys, size)

def splitFrameGenerator(keys, values, targetSize):
    #from marshal import dumps
    dumps = marshal.dumps
    size = 0
    startIndex = 0
    nkeys = len(keys)
    for i in xrange(nkeys):
        k = keys[i]
        v = values[i]
        size += len(dumps(k))+len(dumps(v))
        if size>targetSize:
            endIndex = i+1
            emitK = keys[startIndex:endIndex]
            emitV = values[startIndex:endIndex]
            F = dframe.DataFrameFromLists(emitK, emitV)
            F.setSize(size)
            yield F
            size = 0
            startIndex = endIndex
    if startIndex<nkeys:
        if startIndex<1:
            emitK = keys
            emitV = values
        else:
            emitK = keys[startIndex:]
            emitV = values[startIndex:]
        F = dframe.DataFrameFromLists(emitK, emitV)
        F.setSize(size)
        yield F
    yield None # sentinel

# string key truncation utilities

def truncateKeyStatsInSortedFrames(SortedFrames):
    "generate sorted frames with key min/max estimates truncated to save space in disk index structures"
    # modifies frames in place
    lastFrameMax = None
    lastFrame = None
    #count = 0
    for frame in SortedFrames:
        if frame is None:
            break
        (thisFrameMin, thisFrameMax) = frame.getMinMax()
        #pr "thisframe min and max", repr(thisFrameMin)[:20], repr(thisFrameMax)[:20]
        # finish up last frame
        if lastFrame is not None and lastFrameMax is not None:
            # check order validity and recalculate if apparently violated
            if lastFrameMax>=thisFrameMin:
                # check the actual buffers, the error might be an illusion
                #pr "checking buffers", repr(lastFrameMax)[:20], repr(thisFrameMin)[:20]
                (lastFrameMin, lastFrameMax) = lastFrame.getMinMax(check=True)
                (thisFrameMin, thisFrameMax) = frame.getMinMax(check=True)
                #pr "after checking", repr(lastFrameMax)[:20], repr(thisFrameMin)[:20]
                # if it isn't fixed, the problem is real
                if lastFrameMax>=thisFrameMin:
                    raise ValueError, "sorted frames out of order "+repr((lastFrameMax, thisFrameMin))
            # truncate maximimum in last frame
            lastMaxTruncated = between(lastFrameMax, thisFrameMin)
            #pr "lastMaxTruncated", (lastFrameMax, lastMaxTruncated, thisFrameMin)
            lastFrame.setMinMax(None, lastMaxTruncated) # could inline
            yield lastFrame
            # truncate minimum in frame
            thisMinTruncated = between(lastFrameMax, thisFrameMin, False) # at right if needed.
            ##pr "truncation", (lastFrameMax, thisMinTruncated, thisFrameMin)
            #pr "thisMinTruncated", thisMinTruncated
            frame.setMinMax(thisMinTruncated, None)
        else:
            # first frame
            thisMinTruncated = smaller(thisFrameMin)
            frame.setMinMax(thisMinTruncated, None)
        # move on...
        lastFrameMax = thisFrameMax
        #pr "lastFrameMax now", lastFrameMax
        lastFrame = frame
        #count += 1
    if lastFrame is not None:
        # truncate the max
        lastMaxTruncated = larger(lastFrameMax)
        lastFrame.setMinMax(None, lastMaxTruncated)
        yield lastFrame
    yield None # sentinal

def larger(thing):
    "heuristic for choosing a 'small' thing that sorts at or higher than thing"
    tt = type(thing)
    if tt is TupleType:
        if len(thing)==0:
            return (0,)
        return (larger(thing[0]),)
    elif tt in StringTypes: # elif tt is UnicodeType:
        # cmax should be beyond any reasonable string
        cmax = MAXUNICHR #unichr(0x10fffd)
        try:
            if thing<=cmax:
                return cmax
        except UnicodeDecodeError:
            # unicode conversion error:
            #mch = chr(255)
            #if thing<=mch:
            #    return mch
            # refuse to truncate it
            return thing
        #return cmax * (len(thing)+1) # never happens (?)
        return thing
    #raise ValueError, "larger is only defined for strings and tuples"
    # punt
    return thing

def smaller(thing):
    "heuristic for choosing a 'small' thing that sorts at or lower than thing"
    tt = type(thing)
    if tt is TupleType:
        #if len(thing)==0:
        #    raise ValueError, "cannot find smaller for empty tuple"
        return ()
    elif tt is StringType or tt is UnicodeType:
        #if len(thing)==0:
        #    raise ValueError, "cannot find smaller thing for empty string"
        return ""
    #raise ValueError, "smaller only defined for tuples and strings"
    # punt
    return thing

def between(lowThing, highThing, atLeftOk=True):
    "heuristic for choosing a 'small' object that sorts between lowThing and highThing (possibly at lowThing)"
    # XXX only works for strings or tuples of strings
    #pr "getting between for", repr(lowThing)[:20], repr(highThing)[:20]
    if lowThing>=highThing:
        raise ValueError, "lowthing must be strictly 'bigger' than highthing"
    tl = type(lowThing)
    th = type(highThing)
    if tl not in StringTypes or th not in StringTypes:
        if tl!=th:
            return lowThing
    if tl is TupleType:
        # usual case: they differ on some interior value
        nlow = len(lowThing)
        nhigh = len(highThing)
        for i in xrange( min( nlow, nhigh ) ):
            lowi = lowThing[i]
            highi = highThing[i]
            if lowi!=highi:
                if lowi>highi:
                    raise ValueError, "unreachable code!! "+repr((lowi, highi))
                splitter = between(lowi, highi, atLeftOk)
                if splitter>lowi and splitter<highi:
                    return lowThing[:i] + (splitter,)
                elif i+1<nlow and splitter==lowi:
                    lowi1 = lowThing[i+1]
                    large = larger(lowi1)
                    if large!=lowi1:
                        return lowThing[:i] + (splitter, large)
                    elif atLeftOk:
                        return lowThing
                    else:
                        return highThing
                elif i+1<nhigh and splitter==highi:
                    highi1 = highThing[i+1]
                    small = smaller(highi1)
                    if small!=highi1:
                        return lowThing[:i] + (splitter, small)
                    elif atLeftOk:
                        return lowThing
                    else:
                        return highThing
                elif i+1!=nhigh and i+1!=nlow:
                    #pr "nhigh, nlow, i", nhigh, nlow, i
                    #pr "lowi", lowi
                    #pr "splitter", splitter
                    #pr "highi", highi
                    raise ValueError, "unreachable code"
        # other case: lowThing is prefix of highThing
        if atLeftOk:
            return lowThing
        elif len(highThing)>len(lowThing):
            return lowThing + (smaller( highThing[len(lowThing)] ),)
        else:
            return highThing # unreachable?
    elif tl in StringTypes:
        # look for first difference (don't care about last char)
        ll = len(lowThing)
        lh = len(highThing)
        # prefix special case
        if highThing.startswith(lowThing):
            if atLeftOk:
                return lowThing
            else:
                return highThing[:ll+1]
        for i in xrange( min( ll, lh ) - 1 ):
            lowchar = lowThing[i]
            highchar = highThing[i]
            if lowchar!=highchar:
                if lowchar>highchar:
                    raise ValueError, "this cannot be!"
                #return lowThing[:i] + highchar
                return highThing[:i+1]
        if atLeftOk:
            return lowThing
        else:
            return lowThing + chr(0)
    else:
        # punt
        if atLeftOk:
            return lowThing
        else:
            return highThing

# file format read/write utilities

def storeFramesToFileWithStats(frameGenerator, toFile, atEnd=True):
    "store frame sequence each prefixed by data size and number of elements to file"
    # ONLY ONE STORE SHOULD BE ACTIVE FOR ANY GIVEN FILE AT ANY GIVEN TIME...
    if atEnd:
        toFile.seek(0,2) # eof
    firstSeek = recordSeek = toFile.tell()
    count = 0
    lastFrame = None
    for frame in frameGenerator:
        #pr "storing", repr(frame)
        if frame is None:
            break # end of frames sentinal
        size = frame.getSize()
        #pr "size is", size
        if size>0:
            lastFrame = frame
            # start with a newline, for readability
            if atEnd:
                toFile.seek(0,2) # always store next record at end of file
                recordSeek = toFile.tell()
            else:
                toFile.seek(recordSeek)
            toFile.write("\n")
            prefixSeek = toFile.tell()
            # put placeholders for size and data elements
            dataSize = 0
            prefix = PREFIXFORMAT % (size, dataSize)
            #pr "writing prefix", prefix, "at", toFile.tell()
            toFile.write(prefix)
            # now dump the min/max (may be truncations)
            minmax = frame.getMinMax(check=False)
            (mn, mx) = minmax
            if mn>mx:
                raise ValueError, "won't store bad extrema! "+repr(minmax)
            #pr "dumping min/max at", toFile.tell()
            marshal.dump(minmax, toFile)
            frameSeek = toFile.tell()
            # now write the frame data (may have not been expanded since last read!)
            #pr "storing frame"
            frame.store(toFile)
            endOfRecord = recordSeek = toFile.tell()
            dataSize = endOfRecord-frameSeek
            realprefix = PREFIXFORMAT % (size, dataSize)
            if len(realprefix)!=len(prefix):
                raise ValueError, "logic problem: record prefix changed size!"
            toFile.seek(prefixSeek)
            #pr "writing real prefix", realprefix
            toFile.write(realprefix)
            count+=1
    # store "$" as end sentinal
    toFile.seek(recordSeek)
    toFile.write("$")
    return (count, firstSeek, lastFrame)

def restoreFramesFromFileWithStats(fromFile, startSeek=None, cache=None, noData=False):
    "restore sequence from file as generated by storeToFile, include stats (generator)"
    # cache if present is a dictionary of seek to cached frames
    if startSeek is None:
        startSeek = fromFile.tell()
    done = False
    recordSeek = startSeek
    while not done:
        if cache is not None and cache.has_key(recordSeek):
            # use cached node
            frame = cache[recordSeek]
            recordSeek = frame.endSeek
            if recordSeek is None:
                raise ValueError, "cached frame has no endseek"
            yield frame
        else:
            fromFile.seek(recordSeek)
            mark = fromFile.read(1)
            if mark=="$":
                done = True # end detected
            else:
                if mark!="\n":
                    raise ValueError, "expected newline at beginning of record, found "+repr(mark)
                # read and parse the prefix
                prefix = fromFile.read(PREFIXLENGTH)
                sprefix = prefix.split("&")
                #pr sprefix
                (size, dataSize) = [ int(x) for x in sprefix ]
                # load the min/max
                #pr "loading min/max at", fromFile.tell()
                (minkey, maxkey) = marshal.load(fromFile)
                # load the uninterpretted frame data from file, setting reported stats, and seek
                frame = dframe.DataFrame()
                frame.loadString(fromFile, dataSize, minkey, maxkey, size, recordSeek, noData)
                recordSeek = fromFile.tell()
                if size>0:
                    yield frame
    yield None # end sentinal

def FramesGeneratorFromDFrameFilePaths(filePaths):
    for filePath in filePaths:
        if filePath is None:
            break # for consistancy
        frame = dframe.DataFrame(filePath)
        frame.readOpen()
        yield frame
    yield None # sentinel

# misc utilities

def stats(frameGenerator, statsDictionary):
    "pass on the frames, collecting statistics in dictionary provided"
    # record running information in case of error
    statsDictionary["count"] = 0
    statsDictionary["totalSize"] = 0
    statsDictionary["byteSize"] = 0
    for frame in frameGenerator:
        if frame is None:
            yield None
            break
        statsDictionary["count"] += 1
        statsDictionary["totalSize"] += frame.getSize()
        statsDictionary["byteSize"] += frame.dataSize()
        yield frame
        
