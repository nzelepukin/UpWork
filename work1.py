import pandas as pd
import numpy as np
import aiohttp,time,asyncio,tqdm,string
from bs4 import BeautifulSoup as bs
'''
                                    Case
Get info about indian companies from site http://www.icchk.org.hk/business_directory.php?menu=business and put it in XLS file. 
'''


def catalog_pages(html: str, url: str)->list:
    ''' Parse page and return list of urls of pages '''
    page=bs(html,'lxml')
    try:
        result=page.find('div',attrs={'class','block'}).find_all('div')[-1].find_all('a')[-2].text
        last_page= int(result.rstrip().lstrip())
        return [url+'?page='+str(i) for i in range(1,last_page+1)]
    except:
        return [url]

def parse_catalog(html:str)->list:
    ''' Parse page and return list of items in catalog page '''
    page=bs(html,'lxml')
    result=[i.find('a', href=True)['href'] for i in page.find('div',id='company').find_all('td')]
    return result

async def browsing_get_page(sem, url: str, headers: str, session)->str:
    ''' Get page '''
    async with sem:
        async with session.get(url,headers=headers,ssl=False) as request:
            if request.status==200:
                return await request.text()
            else: return ''

async def browsing_join(url_base: str, headers: str)->list:
    ''' Control asinc getting pages '''
    tasks=[]
    sem=asyncio.Semaphore(200)
    async with aiohttp.ClientSession() as session: 
        for url in url_base:
            task = asyncio.ensure_future(browsing_get_page(sem,url,headers,session))
            tasks.append(task)
        for future in tqdm.tqdm(tasks, total=len(url_base)):
            await future
        return await asyncio.gather(*tasks)

def get_items(htmls: list)->list:
    ''' Take list of catalog HTMLS and return list of URLs '''
    result=[]
    for html in htmls:
        result=result+parse_catalog(html)
    return [ 'http://www.icchk.org.hk/'+i for i in result]

def start_collect()->list:
    ''' Start mechanism collecting htmls '''
    htmls=list()
    urls=list()
    headers={'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
         'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36'}
    url='http://www.icchk.org.hk/business_directory.php?menu=business'
    pages=asyncio.run(browsing_join([url], headers))
    for page in pages:
        urls=urls+catalog_pages(page,url)
    raw_htmls=asyncio.run(browsing_join(urls, headers))
    item_urls=get_items(raw_htmls)
    htmls=asyncio.run(browsing_join(item_urls, headers))
    return htmls

def parse(html: str, temp_base: list)->list:
    ''' Parse page and take fields that you need '''
    soup= bs(html,'lxml')
    tmp=dict()
    a=soup.find_all('tr')[4:-8]
    for row in a:
        row_text = [x.text.strip() for x in row.find_all('td')]
        if len(row_text)>1 and len(row_text[0])<50 and len(row_text[0])<50 :
            tmp[row_text[0]]=row_text[1]
    temp_base.append(tmp)
    return temp_base

def start_parse(htmls: list)->list:
    ''' Return parsed info from all htmls '''
    temp_base=[]
    for html in htmls:
        temp_base=parse(html,temp_base)
    return temp_base

def prettyfy(base: list)->None:
    ''' Return data with format that you need XLS '''
    df=pd.DataFrame(base)
    df.to_excel('out.xlsx')

def start_main()->None: 
    ''' Main function Starts all '''
    start_time=time.time()
    htmls=start_collect()
    base=start_parse(htmls)
    print (base)
    print('--------- {} sec -------'.format(int(time.time()-start_time)))
    prettyfy(base)

start_main()