
"""
Wrapper for specially treated values
"""

import entry
import urllib
#import parameters

class SpecialValue:
    _content = None # for caching
    def __init__(self, flagname, text, archive=None):
#        parameters.alloc(self, ("Sp", flagname, text[:100]))
        #pr "<hr>", self.__class__.__name__, repr(flagname), repr(text), "<hr>"
        self.flagname = flagname
        self.text = text
        self.archive = archive
#    def __del__(self):
#        parameters.dealloc(self)
    def marshalValue(self):
        return (self.flagname, self.text)
    def content(self):
        "content to index (not necessarily display)"
        raise ValueError, "not defined for virtual superclass"
    def xml(self, fieldName):
        return '<fld n="%s" special="%s">%s</fld> <!-- %s -->' % (fieldName, self.flagname, entry.quote(self.text), entry.quote(self.__class__.__name__))
    def html(self, linkMaker=None):
        "html text to display (quoted if needed)"
        raise ValueError, "not defined for virtual superclass"
    def __repr__(self):
        return self.xml("?")
    def __cmp__(self, other):
        # primarily for testing
        myclass = self.__class__
        if not isinstance(other, myclass):
            if hasattr(other, "__class__"):
                return cmp(myclass, other.__class__)
            return cmp(myclass, other) # not sure this is kosher
        return cmp( (self.flagname, self.text), (other.flagname, other.text) )

class IndexedURL(SpecialValue):
    "indexed url: text is url"
    # XXXX modify this to look in file system when appropriate rather than get the url
    def __init__(self, flagname, text, archive=None):
        SpecialValue.__init__(self, flagname, text, archive)
        self.url = text
        if archive:
            self.url = archive.translateURL(text)
    def content(self):
        #pr "<hr>", self.__class__.__name__,"<b>content</b>", repr(self.text), "<hr>"
        c = self._content
        if c is not None:
            return c
        url = self.url
        #url = self.archive.translateURL(url)
        #pr "url translation", (self.text, url)
        f = urllib.urlopen(url)
        c = f.read()
        if not url.lower().startswith("file:"):
            self._content = c
        return c

class ExpandedURL(IndexedURL):
    "show expanded contents in html"
    def html(self, linkMaker=None):
        #pr "<hr>", self.__class__.__name__,"<b>html</b>", repr(self.text), "<hr>"
        c = self.content()
        return entry.quote(c)

class UnExpandedURL(IndexedURL):
    "show link in html, not content"
    def html(self, linkMaker=None):
        #pr "<hr>", self.__class__.__name__,"<b>html</b>", repr(self.text), "<hr>"
        return '<a href="%s">%s</a>' % (self.text, self.text)

class UnIndexedURL(SpecialValue):
    def content(self):
        return "" # don't index anything
    def html(self, linkMaker=None):
        return '<a href="%s">%s</a>' % (self.text, self.text)

class ImageURL(UnIndexedURL):
    def html(self, linkMaker=None):
        return '<img src="%s">' % (self.text,)

class InternalLink(SpecialValue):
    "text of form 'ATTRIBUTE::IDENTITY' where ATTRIBUTE is display attribute"
    _content = None # for caching
    def html(self, linkMaker=None):
        if linkMaker:
            return linkMaker.idLink(self.text)
        return "<b> INTERNAL LINK TO %s </b>" % (self.text,)
    def content(self):
        return ""

SPECIALS = {
    "ExpandedURL": ExpandedURL,
    "UnExpandedURL": UnExpandedURL,
    "UnIndexedURL": UnIndexedURL,
    "ImageURL": ImageURL,
    "InternalLink": InternalLink,
    }
    

def test():
    print
    print ExpandedURL
    eu = ExpandedURL("ExpandedURL", "http://www.xfeedme.com/index.html")
    print "marshal", repr(eu.marshalValue())
    print "content", repr(eu.content())
    print "html", repr(eu.html())
    print "xml", repr(eu.xml("fieldName"))
    print
    print UnExpandedURL
    eu = UnExpandedURL("UnExpandedURL", "http://www.xfeedme.com/index.html")
    print "marshal", repr(eu.marshalValue())
    print "content", repr(eu.content())
    print "html", repr(eu.html())
    print "xml", repr(eu.xml("fieldName"))
    
        
if __name__=="__main__":
    test()
    
