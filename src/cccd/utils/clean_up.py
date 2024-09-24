from bs4 import BeautifulSoup

def html_cleanup(html_text):
    if html_text is None:
        return None
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    for element in soup(['style', 'script','link','svg']):
        element.decompose()
    return str(soup)