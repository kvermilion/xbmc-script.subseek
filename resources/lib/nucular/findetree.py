
"""
Silly module used to find some
implementation of ElementTree under
different Python installations.
"""


etree = None
try:
    import etree
except:
    #print "could not load module etree"
    pass

if etree is None:
    try:
        from xml.etree import ElementTree
        etree = ElementTree
    except:
        #print "could not load module xml.etree.ElementTree"
        pass

if etree is None:
    try:
        import elementtree.ElementTree
        etree = elementtree.ElementTree
    finally:
        if etree is None:
            raise ImportError, (
             "COULD NOT LOCATE ELEMENTTREE IMPLEMENTATION \n"
             "please get it from: http://effbot.org/zone/element-index.htm \n"
             "giving up.")
        
