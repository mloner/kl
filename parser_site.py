# Импортируемые библиотеки
import datetime

from progress.bar import IncrementalBar
from pymongo import MongoClient
from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
import time
import csv


# Получение ответов от сервера
def get_html(url):
    r = requests.get(url)
    return r.text


# Получение всех ссылок с сайта
def get_all_links(html):
    soup = BeautifulSoup(html, 'lxml')

    lis = soup.find('ul', class_='bigline').find_all('li')
    links = []
    for li in lis:
        a = li.find('a').get('href')
        link = 'https://bloknot-volgograd.ru' + a
        comments_count = li.find('a', class_='comcount').text.strip()
        links.append([link, comments_count])

    return links


# Получение инфорации из ссылки с сайта
def get_page_data(html):
    soup = BeautifulSoup(html, 'lxml')
    article = soup.find('article')
    # Получение названия новости
    try:
        name_news = article.find("h1").text.strip()
    except:
        name_news = ''

    # Получение даты новости
    try:
        date_news = soup.find('div', class_='news-item-info').find('span',class_='news-date-time').text
    except:
        date_news = ''

    # Получение ссылки на новость
    try:
        link_news = soup\
          .find('div', class_='nav-wrapper')\
          .find('nav',class_='nav row')\
          .find('div', class_='nav-wrapper')\
          .find('a', class_='link link_nav')\
          .get('href')\
          .replace('/auth/?return_to=', 'https://bloknot-volgograd.ru')
    except:
        link_news = ''

    # Получение текста новости
    try:
        text_elements = article\
                        .find('div', class_='news-text')\
                        .find_all('p')
        ps = []
        for elem in text_elements:
          ps.append(elem.text.strip().replace('\r\n', ''))
        text_news = ' '.join(ps)
    except:
        text_news = ''

    # Получение ссылки на видео из новости
    try:
        video_elems = article.find_all('iframe')
        video_links = []
        for el in video_elems:
          if el.has_attr('allowfullscreen'):
            video_links.append(el.get("src"))
    except:
        pass


    # Документ с данными из новости
    data = {
      'name_news': name_news,
      'date_news': date_news,
      'link_news': link_news,
      'text_news': text_news,
      'link_video': video_links,
      'comments_count' : 0
    }
    return data


# Запись документа в базу данных
def write_mongo(collection, data):
    return collection.insert_one(data).inserted_id


# Обновление(/)
def update_write_mongo(collection, data):
    n = find_document(collection, {"link_news": data['link_news']})
    if (n):
        update_document(collection, n,
                        {
                          "comments_count" : data['comments_count'],
                          "date_news" : data["date_news"]
                        })
        print("True Update")
    else:
        write_mongo(collection, data)
        print("True Write")


# Поиск документа
def find_document(collection, elements, multiple=False):
    if multiple:
        results = collection.find(elements)
        return [r for r in results]
    else:
        if collection.find_one(elements):
            print("True Find")
            return collection.find_one(elements)


# Обновление документа
def update_document(collection, query_elements, new_values):
    collection.update_one(query_elements, {'$set': new_values})


# Главная функция
def main():
    # Подключение к БД
    client = MongoClient('localhost', 27017)
    db = client['VpravdaDB']
    vpravda_collection = db['Vpravda']
    i = 0
    for i in range(10):
        url = 'https://bloknot-volgograd.ru/?PAGEN_1=' + str(i)
        # Списпок ссылок полученных с сайта
        all_links = get_all_links(get_html(url))

        # Прогресс бар для отслеживания прогресса
        bar = IncrementalBar('Countdown', max=len(all_links))

        # Цикл записи в БД
        for url in tqdm(all_links):
            html = get_html(url[0])
            time.sleep(1)
            data = get_page_data(html)
            data['comments_count'] = url[1]
            update_write_mongo(vpravda_collection, data)
        bar.finish()


# Точка входа
if __name__ == '__main__':
    main()