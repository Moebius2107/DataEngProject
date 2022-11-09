#$x("//td[contains(@class,'sl-list-column--filename')]/a/@href")

# from bs4 import BeautifulSoup
# from lxml import html
# import requests
# # page = requests.get('https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AACsy_Ll8IgUjXujQSVR4KUIa/hackathons?dl=0&lst=')
# # tree = html.fromstring(page.content)
# # print (page.content)
# #hackatons = tree.xpath("$x('//td[contains(@class,'sl-list-column--filename')]/a/@href')")
#                         #'//span[@class="item-price"]/text()'

# url = 'https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AACsy_Ll8IgUjXujQSVR4KUIa/hackathons?dl=0&lst='
# r = requests.get(url, allow_redirects=True)
# #print (r.text)

 
import requests
from bs4 import BeautifulSoup


headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }


url = 'https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AACsy_Ll8IgUjXujQSVR4KUIa/hackathons?dl=0&lst='
req = requests.get(url, headers)
soup = BeautifulSoup(req.content, 'html.parser')
#print(soup.prettify())

a= soup.findAll("script")
print(a)
# soup = BeautifulSoup(r.text, features="lxml")
# a =soup.find("div", attrs= {'id': 'pagelet-0'})
# print(a)
# p=a.findAll('div')
# print(p)