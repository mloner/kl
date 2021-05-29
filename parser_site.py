#Импортируемые библиотеки
from progress.bar import IncrementalBar
from pymongo import MongoClient 
from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
import time
import csv

#Получение ответов от сервера
def get_html(url):
  r = requests.get(url)
  return r.text

#Получение всех ссылок с сайта
def get_all_links(html):
  soup = BeautifulSoup(html, 'lxml')
    
  spans = soup.find('div', class_ = 'front_wrapper_top_latest_news').find_all('span', class_='field-content')
  links = []
  for span in spans:
    a = span.find('a').get('href')
    link = 'https://vpravda.ru' + a
    links.append(link)
      
  return links

#Получение инфорации из ссылки с сайта
def get_page_data(html):
  soup = BeautifulSoup(html, 'lxml')
  
  #Получение названия новости
  try:
    name_news = soup.find('h1', class_ = 'page__title title').text.strip()
  except:
    name_news = ''
  
  #Получение даты новости
  try:
    date_news = soup.find('div', class_ = 'field-name-field-article-date').find('span', class_='date-display-single').text
  except:
    date_news = ''
  
  #Получение ссылки на новость
  try:
    link_news = soup.find('link', rel = 'amphtml').get('href')
  except:
    link_news = ''
    
  #Получение текста новости
  try:
    try:
      text_news2 = soup.find('div', class_ = 'field-name-field-article-lead').find('div', class_='field-item').text
    except:
      text_news2 =''
    text_news1 = soup.find('div', class_ = 'field-type-text-with-summary').find('div', class_='field-item').find_all('p')
    ps = []
    ps.append(text_news2.replace('\r\n', ''))
    for p in text_news1:
      ps.append(p.text.replace('\xa0', ' ').replace('\n', '').replace('\r\n\r\n', ''))
    text_news = ' '.join(ps)
  except:
    text_news = ''
    
  #Получение ссылки на видео из новости
  try:
    link_video = soup.find('div', class_ = 'player').find('iframe').get('src')
  except:
    link_video = ''
    
  #Документ с данными из новости
  data = {'name_news':name_news,
          'date_news':date_news,
          'link_news':link_news,
          'text_news':text_news,
          'link_video':link_video}
  return data

#Запись документа в базу данных
def write_mongo(collection, data):
  return collection.insert_one(data).inserted_id

#Обновление(/)
def update_write_mongo(collection, data):
  n = find_document(collection, {"link_news" : data['link_news']})
  if(n):
    update_document(collection, n, {"text_news" : data['text_news']})
    print("True Update")
  else:
    write_mongo(collection, data)
    print("True Write")
#Поиск документа
def find_document(collection, elements, multiple=False):
  if multiple:
    results = collection.find(elements)
    return [r for r in results]
  else:
    if collection.find_one(elements):
      print("True Find")
      return collection.find_one(elements)
#Обновление документа
def update_document(collection, query_elements, new_values):
  collection.update_one(query_elements, {'$set': new_values})

#Главная функция
def main():
  global i
  #Подключение к БД
  client = MongoClient('localhost',27017)
  db = client['VpravdaDB']
  vpravda_collection = db['Vpravda']
  
  #Ссылка на сайт
  url = 'https://vpravda.ru/'
  
  #Списпок ссылок полученных с сайта
  all_links = get_all_links( get_html ( url ) )
  
  #Прогресс бар для отслеживания прогресса
  bar = IncrementalBar('Countdown', max = len(all_links))
  
  #Цикл записи в БД 
  for url in tqdm(all_links):
    html = get_html(url)
    time.sleep(1)
    data = get_page_data(html)
    update_write_mongo(vpravda_collection, data)
  bar.finish()

#Точка входа
if __name__ == '__main__':
  main()