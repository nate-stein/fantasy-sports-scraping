"""
Web-scraping utils.

Attributes:
    month_conversion (dict[str,int]): Maps various string representations of a
    month to an integer.
    html_replacements (dict): Maps HTML characters/strings to the desired
    Python string replacement.
"""
import codecs
import re
import time

from bs4 import BeautifulSoup
import requests
from selenium import webdriver

#####################################################################
__HEADERS = {
    'User-Agent':"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit "
                 "537.36 (KHTML, like Gecko) Chrome",
    'Accept':"text/html, application/xhtml+xml, application/xml;q=0.9, "
             "image/webp,*/*;q=0.8"}

month_conversion = {'jan':1, 'feb':2, 'mar':3, 'apr':4, 'may':5, 'jun':6,
                    'jul':7, 'aug':8, 'sep':9, 'sept':9, 'oct':10, 'nov':11,
                    'dec':12, 'january':1, 'february':2, 'march':3, 'april':4,
                    'june':6, 'july':7, 'august':8, 'september':9, 'october':10,
                    'november':11, 'december':12, 'Jan':1, 'Feb':2, 'Mar':3,
                    'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9,
                    'Sept':9, 'Oct':10, 'Nov':11, 'Dec':12, 'January':1,
                    'February':2, 'March':3, 'April':4, 'June':6, 'July':7,
                    'August':8, 'September':9, 'October':10, 'November':11,
                    'December':12}

html_replacements = {'&nbsp;':' '}


#####################################################################
def get_session () -> requests.Session:
    """Inits Session with frequently used headers."""
    sess = requests.Session()
    sess.headers = __HEADERS
    return sess


def get_selenium_driver (driver_type=None):
    if driver_type == 'phantom':
        return webdriver.PhantomJS()
    elif driver_type == 'chrome' or driver_type is None:
        return webdriver.Chrome()
    else:
        raise ValueError('Unhandled driver_type: {}'.format(driver_type))


def get_http_response (url) -> requests.Response:
    return requests.get(url, headers=__HEADERS)


def get_soup_from_path (path, parser='html.parser') -> BeautifulSoup:
    """Inits BeautifulSoup tag from HTML file located at path."""
    f = codecs.open(path, 'r', encoding='utf-8')
    return BeautifulSoup(f, parser)


def get_soup_from_url (url, parser='html.parser', sleep=0) -> BeautifulSoup:
    """Inits BeautifulSoup out of HTML scraped from url."""
    http_req = get_http_response(url)
    if sleep>0:
        time.sleep(sleep)
    return BeautifulSoup(http_req.text, parser)


def get_selenium_soup (url, parser='html.parser', sleep=0) -> BeautifulSoup:
    """Inits BeautifulSoup out of HTML scraped from url using Selenium."""
    driver = get_selenium_driver()
    driver.get(url)
    if sleep > 0:
        time.sleep(sleep)
    return BeautifulSoup(driver.page_source, parser)


#####################################################################

def clean_html (s, raise_err=True):
    """Cleans text obtained from HTML tags by removing unnecessary space
    or escape characters.

    Args:
        s (str): String to clean.
        raise_err (bool): Optional, default True. If False, then when an
        error is encountered, the original text will be returned. If True,
        the error is raised.

    Returns:
        str: Cleaned text.
    """
    try:
        for old, new in html_replacements.items():
            s = s.replace(old, new)
        # Replace all instances of the newline character (or multiple newline
        # characters) with a space.
        s = re.sub('\n+', ' ', s)
        # Replace all instances of multiple spaces in a row with a single space
        # so all words have one space between them.
        s = re.sub(' +', ' ', s)
        # Encode the content with UTF-8 to eliminate escape characters.
        s = bytes(s, 'UTF-8')
        s = s.decode('ascii', 'ignore')
        return s.strip()
    except Exception as e:
        if raise_err:
            raise e
        else:
            return s
