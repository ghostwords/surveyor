#!/usr/bin/env python3

import argparse
import csv
import re

from datetime import datetime
from multiprocessing import Lock, Process, Queue

import requests

from bs4 import BeautifulSoup


class Logger(object):
    def __init__(self):
        self.lock = Lock()

    def log(self, *args, **kwargs):
        kwargs['flush'] = True
        with self.lock:
            print("[%s]  " % datetime.now(), *args, **kwargs)


class Crawler(object):
    def __init__(self, **kwargs):
        self.log = kwargs['log']
        self.regex = kwargs['regex']
        self.timeout = kwargs['timeout']
        self.url_queue = kwargs['url_queue']

        while not self.url_queue.empty():
            hostname = self.url_queue.get()
            self.check(hostname)

    def fetch(self, url):
        headers = {
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/34.0.1847.131 Safari/537.36'
            )
        }
        response = requests.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def meta_redirect(self, html, url):
        soup = BeautifulSoup(html, 'lxml')

        result = soup.find("meta", attrs={
            "http-equiv": re.compile("Refresh", re.IGNORECASE)
        })

        if result and ";" in result:
            wait, text = result["content"].split(";")
            text = text.strip().lower()
            if text.startswith("url="):
                return text[4:]

        return None

    def get_html(self, hostname):
        html = None

        for proto in ('http', 'https'):
            url = proto + '://' + hostname

            try:
                html = self.fetch(url)

                # check for META redirects
                redirect_url = self.meta_redirect(html, url)
                if redirect_url:
                    # handle relative URLs
                    if redirect_url[0] == '/':
                        redirect_url = url + redirect_url
                    url = redirect_url
                    html = self.fetch(url)

            except (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ContentDecodingError,
                requests.exceptions.HTTPError,
                requests.exceptions.InvalidSchema,
                requests.exceptions.ReadTimeout,
                requests.exceptions.TooManyRedirects,
                requests.packages.urllib3.exceptions.LocationValueError,
                UnicodeError,
            ) as err:
                self.log("%s on %s" % (err, url))
                continue
            else:
                break

        return html

    def check(self, hostname):
        html = self.get_html(hostname)
        if html:
            if self.regex.search(html):
                self.log("!!! Got a match on %s" % hostname)
        else:
            self.log("XXX Failed to fetch %s" % hostname)


def parse_cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("search_regex",
                        help="the regex string to search for")
    parser.add_argument("-l", "--limit", type=int, default=None,
                        help="stop after this many hostnames")
    parser.add_argument("-n", dest='num_crawlers', type=int, default=20,
                        help="how many processes to use in parallel "
                        "(default: %(default)s)")

    return parser.parse_args()


if __name__ == '__main__':
    cli_args = parse_cli_args()

    log = Logger().log
    url_queue = Queue()

    with open('top-1m.csv') as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            url_queue.put(row[1])
            if cli_args.limit:
                if int(row[0]) == cli_args.limit:
                    break

    crawlers = []
    for i in range(cli_args.num_crawlers):
        crawler = Process(
            target=Crawler,
            kwargs={
                'log': log,
                'regex': re.compile(cli_args.search_regex, re.IGNORECASE),
                'timeout': 10,
                'url_queue': url_queue,
            }
        )
        crawler.start()
        crawlers.append(crawler)

    # wait for all processes to finish
    for crawler in crawlers:
        crawler.join()
