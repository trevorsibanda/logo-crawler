from bs4 import BeautifulSoup


class Selector:
    """
    The Selector class is a used to select a subset of a dataframe given a set of
    conditions.
    """    
    def __init__(self, name : str, tag, attr, value) -> None:
        """
        Constructor takes in a name, tag, attribute, and value and assigns them to the object
        
        :param name: The name of the element
        :type name: str
        :param tag: The tag of the element you want to find
        :param attr: The attribute you want to search for
        :param value: The value of the attribute you want to find
        """
        self.name = name
        self.tag = tag
        self.attribute = attr
        self.value = value

    def apply(self, soup: BeautifulSoup):
        """
        apply returns a list of the values of that match the `find` method
        
        :param soup: BeautifulSoup
        :type soup: BeautifulSoup
        :return: A list of all the values of the attribute of the tag.
        """
        result = []
        tags = self.find(soup)
        for t in tags:
            if t.has_attr(self.attribute) :
                result.append((self.value, t.attrs[self.attribute]))
        return result

    def find(self, soup: BeautifulSoup):
        raise NotImplementedError


class LinkSelector(Selector):
    """
    LinkSelector class is a subclass of the `Selector` class that finds all `<link>` tags with a
    `rel` attribute equal to the value passed to the constructor
    """
    def __init__(self, value: str) -> None:
        super().__init__('head>link', 'link', 'href', value)
    
    def find(self, soup: BeautifulSoup):
        return soup.find_all(self.tag, rel=self.value)


class MetaSelector(Selector):
    def __init__(self, value: str) -> None:
        """
        The `MetaSelector` class is a subclass of the `Selector` class that finds all `meta` tags with a
        `property` attribute equal to the `value` attribute
        """
        super().__init__('head>meta', 'meta', 'content', value)
    
    def find(self, soup: BeautifulSoup):
        return soup.find_all(self.tag, property=self.value)

# ImgClassSelector is a selector that relies on an <img /> having a classname including the text "logo" which is commmon practice 
class ImgClassSelector(Selector):
    pass

# CssClassSelector is a Selector that selects elements by their CSS class.
class CssClassSelector(Selector):
    pass

def favicon() -> Selector:
    """
    Returns a selector for the `<link>` element with the `rel` attribute set to
    `icon`
    :return: A selector for the favicon.
    """
    return LinkSelector('icon')

def shortcut_icon() -> Selector:
    """
    Returns a selector for the `<link>` element with the `rel` attribute set to
    `shortcut icon`
    :return: A selector for the shortcut icon.
    """
    return LinkSelector('shortcut icon')

def appletouch() -> Selector:
    """
    Returns a selector that matches all `<link>` elements with a `rel` attribute value of
    `apple-touch-icon`.
    """
    return LinkSelector('apple-touch-icon')

def appletouch_startupimg() -> Selector:
    """
    Returns a selector for the `<link>` element with the `rel` attribute set
    to `apple-touch-startup-image`
    :return: A selector for the link element with the attribute apple-touch-startup-image.
    """
    return LinkSelector('apple-touch-startup-image')

def maskicon() -> Selector:
    """
    Returns a `Selector` object with the `rel` attribute set to `mask-icon`
    :return: A selector for the link element with the attribute mask-icon.
    """
    return LinkSelector('mask-icon')

def fluidicon() -> Selector:
    """
    Returns a selector that selects links with the class `fluid-icon`
    :return: A selector that will find all links with the class 'fluid-icon'
    """
    return LinkSelector('fluid-icon')

def og_logo() -> Selector:
    """
    Returns a `Selector` that will extract the `og:logo` meta tag from a webpage
    :return: The value of the meta tag with the property og:logo
    """
    return MetaSelector('og:logo')

def og_image() -> Selector:
    """
    Returns a selector that selects the `<meta>` tag with the `og:image` property
    :return: A selector object.
    """
    return MetaSelector('og:image')

