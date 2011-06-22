"""
Disjunct ::= CONJUNCT ( "|" CONJUNCT )*
    generates ["OR", sequence ]
CONJUNCT ::= ATOM+
    generates ["AND", sequence ] 
ATOM ::= PREFIX | CONTAINS | CONTAINS_ANY | RANGE | MATCH | PROXIMITY | "(" DISJUNCT ")" | NEGATION
    generates same
NEGATION ::= ~ ATOM
    generates ["NOT", atom]
PREFIX ::= NAME "=" NAME ".."
    generates ["prefixAttribute", attributeName, prefix]
CONTAINS ::= NAME ":" NAME
    generates ["attributeWord", attributeName, word]
CONTAINS_ANY ::= NAME
    generates ["anyWord", name]
RANGE ::= NAME "=" "[" NAME ":" NAME "]"
    generates ["attributeRange, attributeName, fromName, toName]
MATCH ::= NAME "=" NAME
    generates ["matchAttribute", attributeName, value]
PROXIMITY ::= "<" NUMBER ">" NAME ( ".." NAME )+
    generates ["proximateWords", number, [words..]]
NAME ::= alphanumeric sequence | '"' anything with double quotes escaped '"'
NUMBER ::= numeric sequence
"""

import string

def parseAtom(text, cursor):
    start = cursor
    (cursor,w) = skipws(text, cursor)
    ltext = len(text)
    assert cursor<ltext, "cannot find atom past end of string"
    first = text[cursor]
    if first=="<":
        cursor += 1
        # parse a proximity
        (cursor, number) = findNumber(text, cursor)
        assert number is not None, 'expect "<" NUMBER ">" NAME ( ".." NAME )+ parsing number '+repr(text[start:start+80])
        (cursor, w) = skipws(text, cursor)
        assert text[cursor:cursor+1]==">", 'expect "<" NUMBER ">" NAME ( ".." NAME )+ looking for > '+repr(text[start:start+80])
        cursor+=1
        # parse at least one word
        words = []
        done = False
        while not done:
            (cursor, w) = skipws(text, cursor)
            (cursor, word) = findName(text, cursor)
            assert word is not None, 'expect "<" NUMBER ">" NAME ( ".." NAME )+ looking for NAME '+repr(text[start:start+80])
            words.append(word)
            (cursor, w) = skipws(text, cursor)
            if text[cursor:cursor+2]=="..":
                done = False
                cursor += 2
            else:
                done = True
        return (cursor, ["proximateWords", int(number), words])
    elif first=="~":
        # parse negation
        cursor+=1
        (cursor, w) = skipws(text, cursor)
        (cursor, atom) = parseAtom(text, cursor)
        return (cursor, ["NOT", atom])
    elif first=="(":
        # parse parenthesized disjunct
        cursor+=1
        (cursor, w) = skipws(text, cursor)
        (cursor, result) = parseDisjunct(text, cursor)
        (cursor, w) = skipws(text, cursor)
        assert text[cursor]==")", 'expect "(" DISJUNCT ")", looking for ")" at '+repr((cursor, text[cursor:cursor+80]))
        cursor+=1
        return (cursor, result)
    else:
        # pass something starting with a name
        (cursor, name) = findName(text, cursor)
        assert name is not None, "expected name at "+repr((cursor, text[cursor:cursor+80]))
        (cursor, w) = skipws(text, cursor)
        first = ""
        if cursor<ltext:
            first = text[cursor]
        if first==":":
            # parse contains
            cursor+=1
            (cursor, w) = skipws(text, cursor)
            # parse contains
            (cursor, name2) = findName(text, cursor)
            assert name2 is not None, 'expect NAME ":" NAME at '+repr((cursor, text[cursor:cursor+80]))
            return (cursor, ["attributeWord", name, name2])
        elif first=="=":
            # parse match or prefix or range
            cursor+=1
            if text[cursor:cursor+1]=="[":
                cursor+=1
                (cursor, w) = skipws(text, cursor)
                (cursor, name2) = findName(text, cursor)
                if name2 is None:
                    name2=""
                (cursor, w) = skipws(text, cursor)
                assert text[cursor:cursor+1]==":", 'expect NAME "=" "[" NAME2 ":" NAME3 "]" looking for ":" NAME3 at '+repr(
                    (cursor, text[cursor:cursor+80]))
                cursor+=1
                (cursor, w) = skipws(text, cursor)
                (cursor, name3) = findName(text, cursor)
                if name3 is None:
                    name3=""
                (cursor, w) = skipws(text, cursor)
                assert text[cursor:cursor+1]=="]", 'expect NAME "=" "[" NAME2 ":" NAME3 "]" looking for "]" at '+repr(
                    (cursor, text[cursor:cursor+80]))
                cursor+=1
                result = (cursor, ["attributeRange", name, name2, name3])
                #pr "range result", result
                return result                
            (cursor, w) = skipws(text, cursor)
            (cursor, name2) = findName(text, cursor)
            if name2 is None:
                name2=""
            elif text[cursor:cursor+2]=="..":
                cursor+=2
                return (cursor, ["prefixAttribute", name, name2])
            return (cursor, ["matchAttribute", name, name2])
        else:
            # parse contains_any
            return (cursor, ["anyWord", name])

def parseConjunct(text, cursor):
    L = []
    ltext = len(text)
    done = False
    while not done:
        (cursor, atom) = parseAtom(text,cursor)
        #pr "conjunct finds atom", atom
        L.append(atom)
        (cursor, w) = skipws(text, cursor)
        #pr "conjunct skipped ws", repr(w)
        if cursor>=ltext or text[cursor] in "|)":
            #pr "conjunct terminated at", cursor, repr(text[cursor:])
            done = True
    if not L:
        raise ValueError, "empty disjunct is not permitted"
    if len(L)==1:
        return (cursor, L[0])
    return (cursor, ["AND", L])

def parseQuery(text, cursor=0):
    (cursor, result) = parseDisjunct(text, cursor)
    if cursor!=len(text):
        raise ValueError, "text not all consumed "+repr((cursor, len(text), text[cursor:cursor+80]))
    return result

def parseDisjunct(text, cursor):
    L = []
    ltext = len(text)
    done = False
    while cursor<ltext and not done:
        (cursor, conj) = parseConjunct(text, cursor)
        L.append(conj)
        (cursor, w) = skipws(text, cursor)
        if cursor<ltext and text[cursor]=="|":
            cursor+=1
        else:
            done = True
    if not L:
        raise ValueError, "empty disjunct is not permitted"
    if len(L)==1:
        return (cursor, L[0])
    return (cursor, ["OR", L])

def skipws(text, cursor, ws=string.whitespace):
    return findName(text, cursor, ws, False)

def findNumber(text, cursor, nm=string.digits):
    return findName(text, cursor, nm, False)

def findName(text, cursor, an=string.letters+string.digits, quotes=True):
    name = None
    ltext = len(text)
    start = cursor
    while cursor<ltext and text[cursor] in an:
        cursor += 1
    if start<cursor:
        name = text[start:cursor]
    elif quotes and cursor<ltext and text[cursor]=='"':
        # try to parse quoted sequence
        cursor+=1
        L = []
        while cursor<ltext and text[cursor]!='"':
            if text[cursor]=="\\" and text[cursor+1:cursor+2] in ("\\", '"'):
                cursor+=1
            L.append(text[cursor])
            cursor+=1
        assert cursor<ltext and text[cursor]=='"', "couldn't close quotes "+repr((start, text[start:start+50]))
        cursor += 1
        name = "".join(L)
    return (cursor, name)

# ==== testing stuff

def testparse(text, expected=None):
    import pprint
    print "# test parse for ", repr(text)
    p = parseQuery(text)
    print "testparse(" +repr(text), ","
    pprint.pprint(p)
    print ")"
    if expected is not None:
        assert p==expected

def test():
    testparse(' avv ' ,
              ['anyWord', 'avv']
              )
    testparse('ayyyn b' ,
              ['AND', [['anyWord', 'ayyyn'], ['anyWord', 'b']]]
              )
    testparse('a | bmmm' ,
              ['OR', [['anyWord', 'a'], ['anyWord', 'bmmm']]]
              )
    testparse('~a' ,
              ['NOT', ['anyWord', 'a']]
              )
    testparse('a=b..' ,
              ['prefixAttribute', 'a', 'b']
              )
    testparse('a=b' ,
              ['matchAttribute', 'a', 'b']
              )
    testparse('a:b' ,
              ['attributeWord', 'a', 'b']
              )
    testparse('a=[b:c]' ,
              ['attributeRange', 'a', 'b', 'c']
              )
    testparse('<3> a .. b' ,
              ['proximateWords', 3, ['a', 'b']]
              )
    testparse(' "quoted \\"name\\"" ' ,
              ['anyWord', 'quoted "name"']
              )
    testparse(' avv ~ bxx ' ,
              ['AND', [['anyWord', 'avv'], ['NOT', ['anyWord', 'bxx']]]]
              )
    testparse('~ (ayyyn b) www' ,
              ['AND',
               [['NOT', ['AND', [['anyWord', 'ayyyn'], ['anyWord', 'b']]]],
                ['anyWord', 'www']]]
              )
    testparse('~ (ayyyn b) | www' ,
              ['OR',
               [['NOT', ['AND', [['anyWord', 'ayyyn'], ['anyWord', 'b']]]],
                ['anyWord', 'www']]]
              )
    testparse('a | bmmm rrr=xxx..' ,
              ['OR',
               [['anyWord', 'a'],
                ['AND', [['anyWord', 'bmmm'], ['prefixAttribute', 'rrr', 'xxx']]]]]
              )

if __name__=="__main__":
    test()
    
