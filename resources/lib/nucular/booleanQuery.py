"""
support for boolean query syntax
"""

# XXXX negation could be optimized to pass in the
#   positive range of interest as the initial result
#   (this would require some generalization of the query evaluation).

import parseQuery

def booleanResult(queryString, session):
    parse = parseQuery.parseQuery(queryString)
    result = getResult(parse, session, queryString)
    return result

def getResult(parse, session, qs):
    indicator = parse[0]
    assert indicator!="NOT", "unrestricted negation not permitted "+repr((qs, parse))
    if indicator=="OR":
        [indicator, sequence] = parse
        return disjunction(sequence, session, qs)
    elif indicator=="AND":
        [indicator, sequence] = parse
        return conjunction(sequence, session, qs)
    else:
        sequence = [parse]
        return conjunction(sequence, session, qs)

def disjunction(sequence, session, qs):
    assert sequence, "null query not allowed "+repr(qs)
    parse0 = sequence[0]
    result = getResult(parse0, session, qs)
    for parse in sequence[1:]:
        presult = getResult(parse, session, qs)
        result.unionDict(presult.idDict)
    return result

def conjunction(sequence, session, qs):
    #pr "interpreting conjuction", sequence
    myQuery = session.Query()
    negativeParses = []
    positiveParses = []
    positiveResults = []
    for parse in sequence:
        indicator = parse[0]
        if indicator=="NOT":
            [indicator, np] = parse
            #pr "negative", np
            negativeParses.append(np)
        elif indicator=="prefixAttribute":
            positiveParses.append(parse)
            [indicator, attr, prefix] = parse
            myQuery.prefixAttribute(attr, prefix)
        elif indicator=="attributeWord":
            positiveParses.append(parse)
            [indicator, attr, word] = parse
            myQuery.attributeWord(attr, word)
        elif indicator=="anyWord":
            positiveParses.append(parse)
            [indicator, word] = parse
            myQuery.anyWord(word)
        elif indicator=="attributeRange":
            positiveParses.append(parse)
            [indicator, attr, lower, upper] = parse
            myQuery.attributeRange(attr, lower, upper)
        elif indicator=="matchAttribute":
            positiveParses.append(parse)
            [indicator, attr, val] = parse
            myQuery.matchAttribute(attr, val)
        elif indicator=="proximateWords":
            positiveParses.append(parse)
            [indicator, nearlimit, words] = parse
            myQuery.proximateWords(words, nearlimit)
        else:
            # xxxx this could be optimized to operate on relevant domain for conjunction
            presult = getResult(parse, session, qs)
            positiveResults.append(presult)
    # get the query result
    if positiveParses:
        (result, status) = myQuery.evaluate()
    else:
        if positiveResults:
            result = positiveResults[0]
            positiveResults = positiveResults[1:]
        else:
            if negativeParses:
                assert False, "unrestricted negative queries are not allowed "+repr((qs, sequence))
            else:
                assert False, "null query is not allowed "+repr((qs, sequence))
    assert result is not None, "failed to evaluate query with status="+repr(status)
    # restrict using additional positive conditions
    for pr in positiveResults:
        if result.idDict:
            result.intersectDict(pr.idDict)
    # restrict using negations
    for np in negativeParses:
        if result.idDict:
            nresult = getResult(np, session, qs)
            nDict = nresult.idDict
            result.differenceDict(nDict)
    return result
            

