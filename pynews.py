from selenium import webdriver
import time
import unicodecsv as csv
import logging
import openpyxl
import os
import sys
from multiprocessing.pool import ThreadPool
import Queue
from selenium.common.exceptions import NoSuchElementException


def setup_logger(logger_name, log_file=None, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.addHandler(streamHandler)
    if log_file:
        fileHandler = logging.FileHandler(log_file, mode='w')
        fileHandler.setFormatter(formatter)
        l.addHandler(fileHandler)
    l.setLevel(level)

setup_logger('console')
setup_logger('log', 'error.log')
console = logging.getLogger('console')
log = logging.getLogger('log')


def bing(term):
    rows = []
    try:
        driver = webdriver.PhantomJS()
        url = "https://www.bing.com/news/search?q=" + term + "&qft=interval%3d%227%22+sortbydate%3d%221%22&form=PTFTNR"
        console.info("scraping: %s" % url)
        driver.get(url)
        for i in range(1, 11):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
        for article in driver.find_elements_by_css_selector('div.newsitem'):
            try:
                a = article.find_element_by_css_selector('a.title')
                link = a.get_attribute('href')
                title = a.text
                excerpt = article.find_element_by_css_selector('div.snippet').text
                rows.append(["Bing", term, link, title, excerpt])
            except NoSuchElementException:
                pass
        driver.close()
        console.info("Bing found %d news articles for term %s" % (len(rows), term))
    except:
        log.exception("Bing exception for term: %s, passing" % term)
        pass
    finally:
        return rows


def yahoo(term):
    rows = []
    try:
        driver = driver = webdriver.PhantomJS()
        url = 'https://news.search.yahoo.com/search;_ylt=AwrC0CbI0x9Xz1wA5VXQtDMD;_ylu=X3oDMTB0bTc0NHA4BGNvbG8DYmYxBHBvcwMx' \
              'BHZ0aWQDBHNlYwNzb3J0?p=' + term + '&ei=UTF-8&flt=age%3A1d%3Branking%3Adate%3B&type=pivot_us_srp_yahoonews&fr' \
                                                 '=uh3_news_web_gs'
        console.info("Scraping: %s" % url)
        driver.get(url)
        for i in range(1, 11):
            for article in driver.find_elements_by_css_selector('div.NewsArticle'):
                try:
                    a = article.find_element_by_css_selector('div.compTitle a')
                    link = a.get_attribute('href')
                    title = a.text
                    excerpt = article.find_element_by_css_selector('div.compText').text
                    rows.append(["Yahoo", term, link, title, excerpt])
                except NoSuchElementException:
                    pass
            try:
                driver.find_element_by_css_selector('div.compPagination > a.next').click() #next page
            except NoSuchElementException:
                logging.info("no next page")
                break
        console.info("Yahoo found %d news articles for term %s" % (len(rows), term))
        driver.close()
    except:
        log.exception("Yahoo exception for term: %s, passing" % term)
    finally:
        return rows


def print_usage():
    print("Usage: " + sys.argv[0] + " <input file>")
    print("""
The input file is an excel file with one search term per row
""")


def read_terms(excel_file):
    sheet = openpyxl.load_workbook(excel_file).worksheets[0]
    terms = []
    for row in sheet:
        terms.append(row[0].value)
    return terms


def imap_queue(f, terms, queue):
    output = ThreadPool(1).imap(f, terms)
    for rows in output:
        queue.put(rows)
    return


def write_output(output_queue, output_file):
    writer = csv.writer(output_file, delimiter=',', encoding='utf-8')
    while True:
        rows = output_queue.get()
        writer.writerows(rows)
        output_queue.task_done()


def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(0)

    terms = read_terms(os.path.abspath(sys.argv[1]))
    output_queue = Queue.Queue()
    output_file = 'output_%s.csv' % time.strftime("%m-%d-%Y-%H%M%S")
    output_file = open(output_file, 'wb')
    try:
        output_writer = ThreadPool(1).apply_async(write_output, (output_queue, output_file))
        yahoo_results = ThreadPool(1).apply_async(imap_queue, (yahoo, terms, output_queue))
        bing_results = ThreadPool(1).apply_async(imap_queue, (bing, terms, output_queue))
        yahoo_results.get()
        bing_results.get()
        output_queue.join()
    finally:
        output_file.close()

if __name__ == '__main__':
    main()
