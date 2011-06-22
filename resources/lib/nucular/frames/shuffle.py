
#import bisect

FAST = True

if __name__ == '__main__' and FAST:
    # Import Psyco if available
    try:
        #raise ImportError # for test
        import psyco
        psyco.full()
        print "WITH PSYCO"
    except ImportError:
        print "NO PSYCO"
        pass
    # ...your code here...

#debugcounter = [0]

def shuffleMergeIndices(keys1, values1, index1, keys2, values2, index2, preferFirst=True):
    """
    return (mkeys, mvalues, index1, index2) for merged values and unmerged remainder.
    """
    # this version is optimized to reduce "tail slicing" when input lists are large
    len1 = len(keys1)
    len2 = len(keys2)
    #pr  "shufflemerge indices", index1, index2, "lengths", len1, len2
    #debugcounter[0]+=1
    #if debugcounter[0]>100: raise "debug halt"
    if keys1[-1]<keys2[index2]:
        # optimized case: all of keys1 remainder is smaller than keys2 remainder
        mkeys = keys1
        mvalues = values1
        if index1>0:
            mkeys = keys1[index1:]
            mvalues = values1[index1:]
        #pr  "choose tail 1", len1, index2
        return (mkeys, mvalues, len1, index2)
    if keys2[-1]<keys1[index1]:
        # optimized case: all of keys2 remainder is smaller than keys1 remainder
        mkeys = keys2
        mvalues = values2
        if index2>0:
            mkeys = keys2[index2:]
            mvalues = values2[index2:]
        #pr  "choose tail 2", index1, len2
        return (mkeys, mvalues, index1, len2)
    # otherwise merge to smallest largest
    # this algorithm attempts to avoid list reallocation or too-large allocations
    chunkSize = 1000
    keysOut = [None]*chunkSize
    valuesOut = [None]*chunkSize
    outIndex = 0
    pairs = []
    while index1<len1 and index2<len2:
        key1 = keys1[index1]
        key2 = keys2[index2]
        same = key1==key2
        if key1<key2 or (same and preferFirst):
            outkey = key1
            outvalue = values1[index1]
            index1 += 1
            #pr "choice1", (outkey, outvalue)
            if same:
                index2+=1
        else:
            outkey = key2
            outvalue = values2[index2]
            index2 += 1
            #pr "choice2", (outkey, outvalue)
            if same:
                index1 += 1
        keysOut[outIndex] = outkey
        valuesOut[outIndex] = outvalue
        outIndex += 1
        # dump the outs if chunk is full
        if outIndex>=chunkSize:
            pairs.append( (keysOut, valuesOut) )
            keysOut = [None]*chunkSize
            valuesOut = [None]*chunkSize
            outIndex = 0
    # if there were no pairs, simple return
    if not pairs:
        mkeys = keysOut[:outIndex]
        mvalues = valuesOut[:outIndex]
        #pr  "  shuffled to indices", index1, index2
        return (mkeys, mvalues, index1, index2)
    # otherwise must unpack pairs
    npairs = len(pairs)
    totalSize = outIndex + chunkSize*npairs
    mkeys = [None]*totalSize
    mvalues = [None]*totalSize
    index = 0
    for (ks, vs) in pairs:
        nextIndex = index+chunkSize
        mkeys[index:nextIndex] = ks
        mvalues[index:nextIndex] = vs
        index = nextIndex
    # add remainder
    lastIndex = index+outIndex
    if lastIndex!=totalSize:
        raise ValueError, "logic problem: didn't reach last index"
    mkeys[index:] = keysOut[:outIndex]
    mvalues[index:] = valuesOut[:outIndex]
    #pr  "   shuffled in chunks to indices", index1, index2
    return (mkeys, mvalues, index1, index2)

def shuffleMergeI(keys1, values1, keys2, values2, preferFirst=True):
    "version of shuffleMerge implemented using shuffleMergeIndices (primarily for testing)"
    (mkeys, mvalues, index1, index2) = shuffleMergeIndices(
        keys1, values1, 0, keys2, values2, 0, preferFirst)
    if index1==len(keys1):
        return (mkeys, mvalues, None, None, keys2[index2:], values2[index2:])
    elif index2==len(keys2):
        return (mkeys, mvalues, keys1[index1:], values1[index1:], None, None)
    else:
        raise ValueError, "one of the two lists should always be exhausted"

def shuffleMerge(keys1, values1, keys2, values2, preferFirst=True):
    """
    return (mergedKeys, matchingValues, remainderKeys1, remainderValues1, remainderKeys2, remainderValues2)
    if preferFirst then choose value1 on equality, otherwise value2
    """
    # this version explicitly iterates over the keys
    #remainderKeys1 = remainderValues1 = remainderKeys2 = remainderValues2 = None
    # short cut if the lists don't intersect.  XXXX maybe should make copies for safety?
    if keys1[-1]<keys2[0]:
        return (keys1, values1, None, None, keys2, values2)
    if keys2[-1]<keys1[0]:
        return (keys2, values2, keys1, values1, None, None)
    # otherwise do a merge
    len1 = len(keys1)
    len2 = len(keys2)
    count1 = count2 = 0
    lenOut = len1+len2
    keysOut = [None]*lenOut
    valuesOut = [None]*lenOut
    countOut = 0
    while count1<len1 and count2<len2:
        k1 = keys1[count1]
        k2 = keys2[count2]
        #pr   "at", (k1, k2, count1, count2, countOut)
        same = (k1==k2)
        if k1<k2 or same and preferFirst:
            #pr   "k1 smaller"
            keysOut[countOut] = k1
            valuesOut[countOut] = values1[count1]
            count1 += 1
            if same:
                count2 += 1
        else:
            #pr   "k2 smaller"
            keysOut[countOut] = k2
            valuesOut[countOut] = values2[count2]
            count2 += 1
            if same:
                count1 += 1
        countOut+=1
    if count1<len1:
        remainderKeys1 = keys1[count1:]
        remainderValues1 = values1[count1:]
        #pr   "k1 remainder", count1, len1, remainderKeys1, remainderValues1
    else:
        remainderKeys1 = remainderValues1 = None
    if count2<len2:
        remainderKeys2 = keys2[count2:]
        remainderValues2 = values2[count2:]
        #pr   "k2 remainder", count2, len2, remainderKeys2, remainderValues2
    else:
        remainderKeys2 = remainderValues2 = None
    if countOut<lenOut:
        mergedKeys = keysOut[:countOut]
        mergedValues = valuesOut[:countOut]
        #pr   "merged truncated", countOut, lenOut, mergedKeys, mergedValues
    else:
        # never happens?
        mergedKeys = keysOut
        mergedValues = valuesOut
    result = (mergedKeys, mergedValues, remainderKeys1, remainderValues1, remainderKeys2, remainderValues2)
    return result

def shuffleMerge0(keys1, values1, keys2, values2):
    # this version uses list.sort (and its usually slower)
    import bisect
    lastKey1 = keys1[-1]
    lastKey2 = keys2[-1]
    swap = lastKey1>lastKey2
    if swap:
        #pr  "swapping", lastKey1, lastKey2
        (keys1, values1, keys2, values2, lastKey1, lastKey2) = (keys2, values2, keys1, values1, lastKey2, lastKey1)
    # now lastKey1 is in range of keys2: truncate keys2
    # merge all of keys1/values1
    part1 = zip(keys1, values1)
    #pr  "part1", part1
    # merge part of keys2, values2
    endindex = bisect.bisect(keys2, lastKey1)
    #pr  "endindex", endindex, len(keys2), lastKey1, keys2
    part2 = zip(keys2[:endindex], values2[:endindex])
    #pr  "part2", part2
    all = part1+part2
    all.sort()
    #pr  "all", all
    mergedKeys = [k for (k,v) in all]
    mergedValues = [v for (k,v) in all]
    keys1 = None
    values1 = None
    keys2 = keys2[endindex:]
    values2 = values2[endindex:]
    #pr  "before swap", (keys1, values1, keys2, values2)
    if swap:
        #pr  "unswapping"
        (keys1, values1, keys2, values2) = (keys2, values2, keys1, values1)
    #pr  "after swap", (keys1, values1, keys2, values2)
    return (mergedKeys, mergedValues, keys1, values1, keys2, values2)

def generateShuffledPairs(leafGenerators, desiredSize=1000):
    currentGenerators = []
    currentListsOfKeys = []
    currentListsOfValues = []
    currentIndices = []
    #pr "setting up generateShufflePairs"
    for generator in leafGenerators:
        leaf = generator.next()
        if leaf is not None:
            currentGenerators.append(generator)
            (keys, values) = leaf.sortedKeysAndValues()
            currentListsOfKeys.append(keys)
            currentListsOfValues.append(values)
            currentIndices.append(0)
    #pr "set up done"
    currentLengths = [ len(keys) for keys in currentListsOfKeys ]
    # buffer outputs to avoid tiny pairs (makes the rest of the pipeline work better)
    outCount = 0
    outPairs = []
    while len(currentGenerators)>1:
        (mergedKeys, mergedValues, exhaustedIndex, indices, listOfLengths) = multiShuffleMerge(
            currentListsOfKeys, currentListsOfValues, currentIndices, currentLengths)
        exhaustedGenerator = currentGenerators[exhaustedIndex]
        nextleaf = exhaustedGenerator.next()
        if nextleaf is None:
            #pr "deleting", exhaustedIndex
            del currentGenerators[exhaustedIndex]
            del currentListsOfKeys[exhaustedIndex]
            del currentListsOfValues[exhaustedIndex]
            del currentIndices[exhaustedIndex]
            del currentLengths[exhaustedIndex]
        else:
            #pr "advancing", exhaustedIndex
            (keys, values) = nextleaf.sortedKeysAndValues()
            currentListsOfKeys[exhaustedIndex] = keys
            currentListsOfValues[exhaustedIndex] = values
            currentIndices[exhaustedIndex] = 0
            currentLengths[exhaustedIndex] = len(keys)
        if mergedKeys:
            #yield (mergedKeys, mergedValues)
            outCount += len(mergedKeys)
            outPairs.append( (mergedKeys, mergedValues) )
            if outCount>desiredSize:
                outKeys = [None]*outCount
                outValues = [None]*outCount
                cursor = 0
                for (keys, values) in outPairs:
                    nkeys = len(keys)
                    nextcursor = cursor+nkeys
                    outKeys[cursor:nextcursor] = keys
                    outValues[cursor:nextcursor] = values
                    cursor = nextcursor
                yield (outKeys, outValues)
                outCount = 0
                outPairs = []
    # unload remaining buffered output
    if outCount:
        outKeys = [None]*outCount
        outValues = [None]*outCount
        cursor = 0
        for (keys, values) in outPairs:
            nkeys = len(keys)
            nextcursor = cursor+nkeys
            outKeys[cursor:nextcursor] = keys
            outValues[cursor:nextcursor] = values
            cursor = nextcursor
        yield (outKeys, outValues)
    # unload any remaining stored keys and values
    for (keys, values, index) in zip(currentListsOfKeys, currentListsOfValues, currentIndices):
        #pr "unloading remainder", len(keys)-index
        yield (keys[index:], values[index:])
    for generator in currentGenerators:
        #pr "dumping final tree"
        leaf = generator.next()
        while leaf is not None:
            pair = leaf.sortedKeysAndValues()
            yield pair
            leaf = generator.next()
    # signal completion
    #pr "generateShuffledPairs done"
    yield None

def multiShuffleMerge(listsOfKeys, listsOfValues, indices, listOfLengths=None):
    "return (mergedKeys, mergedValues, exhaustedIndex, indices, listOfLengths)"
    if listOfLengths is None:
        listOfLengths = [len(keys) for keys in listsOfKeys]
    exhaustedIndex = None
    nlists = len(listsOfKeys)
    rlists = range(nlists)
    outputCount = 0
    #utputKeys = {}
    #outputValues = {}
    mergedKeys = []
    mergedValues = []
    while exhaustedIndex is None:
        currentKeys = [ listsOfKeys[i][indices[i]] for i in rlists ]
        minCurrentKey = min(currentKeys)
        minIndex = currentKeys.index(minCurrentKey)
        minCursor = indices[minIndex]
        associatedValue = listsOfValues[minIndex][minCursor]
        #outputKeys[outputCount] = minCurrentKey
        #outputValues[outputCount] = associatedValue
        mergedKeys.append(minCurrentKey)
        mergedValues.append(associatedValue)
        outputCount += 1
        nextCursor = indices[minIndex] = minCursor+1
        if nextCursor>=listOfLengths[minIndex]:
            exhaustedIndex = minIndex
    #mergedKeys = [ outputKeys[i] for i in xrange(outputCount) ]
    #mergedValues = [ outputValues[i] for i in xrange(outputCount) ]
    return (mergedKeys, mergedValues, exhaustedIndex, indices, listOfLengths)

def shuffleMergeX(keys1, values1, keys2, values2, preferFirst=True):
    "testing multishuffle by implementing shufflemerge using it..."
    if not preferFirst:
        raise ValueError, "preferFirst of false not supported by this implementation"
    indices = [0,0]
    listsOfKeys = [keys1, keys2]
    listsOfValues = [values1, values2]
    (mergedKeys, mergedValues, exhaustedIndex, indices, listOfLengths) = multiShuffleMerge(
        listsOfKeys, listsOfValues, indices)
    [index1, index2] = indices
    keys1 = keys1[index1:]
    values1 = values1[index1:]
    keys2 = keys2[index2:]
    values2 = values2[index2:]
    if not keys1:
        return (mergedKeys, mergedValues, None, None, keys2, values2)
    else:
        return (mergedKeys, mergedValues, keys1, values1, None, None)

JUNK = [0]

def testList(N, data):
    import md5
    ##pr JUNK
    S = str(JUNK)
    JUNK[0]+=1
    #return [tuple(md5.md5(str(i)+data+S).hexdigest()) for i in xrange(N)]
    return [(md5.md5(str(i)+data+S).hexdigest()) for i in xrange(N)]

#def testList(N, data):
#    return [data+str(i) for i in xrange(N)]

def timingTests():
    from time import time
    for size in (1, 10, 100, 1000, 10000, 100000):
      for jj in range(4):
        print;
        print "testing size", size, jj
        keys1 = testList(size, "keys1_"+repr(size))
        values1 = testList(size, "values1_"+repr(size))
        keys2 = testList(size, "KEYS2_"+repr(size))
        values2 = testList(size, "VALUES2_"+repr(size))
        keys1.sort()
        keys2.sort()
        result = oldElapsed = None
        for f in (shuffleMerge, shuffleMergeI, shuffleMerge0, shuffleMergeX):
            now = time()
            r = f(keys1, values1, keys2, values2)
            elapsed = time()-now
            print "  for", f, "elapsed", elapsed
            if result and result!=r:
                print "old result and new result differ"
                print "inputs"
                print "keys1=", keys1
                print "values1=", values1
                print "keys2=", keys2
                print "values2=", values2
                print
                print "new results"
                for x in result:
                    print "  ", x
                print "old result is"
                for x in r:
                    print "  ", x
                if len(r)!=len(result):
                    print "result lengths differ", len(r), len(result)
                else:
                    for i in xrange(len(r)):
                        ri = r[i]
                        resulti = result[i]
                        if ri!=resulti:
                            print "difference is at result index", i
                            if ri is None:
                                print "old is None"
                            elif resulti is None:
                                print "new is None"
                            else:
                                if len(ri)!=len(resulti):
                                    print "lengths differ", len(ri), len(resulti)
                                else:
                                    for j in xrange(len(ri)):
                                        rij = ri[j]
                                        resultij = resulti[j]
                                        if rij!=resultij:
                                            print "at index", i, j, "differing components", "new", resultij, "old", rij
                raise ValueError, "results differ"
            if oldElapsed is not None and oldElapsed>0:
                print "ratio of second/first", elapsed/oldElapsed
            result = r
            oldElapsed = elapsed
            
if __name__=="__main__":
    timingTests()
