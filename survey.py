#!/usr/bin/env python3

# surveyor
#
# Copyright 2016 ghostwords.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import csv
import re

from datetime import datetime
from multiprocessing import Lock, Process, Queue

import colorama
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

        url = 'http://' + hostname

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

        return html

    def check(self, hostname):
        html = self.get_html(hostname)
        if html:
            if self.regex.search(html):
                self.log("%s!!! Got a match on %s%s" % (
                    colorama.Back.GREEN + colorama.Fore.BLACK,
                    hostname,
                    colorama.Style.RESET_ALL,
                ))
        else:
            self.log("%sXXX%s Failed to fetch %s" % (
                colorama.Fore.RED + colorama.Style.BRIGHT,
                colorama.Style.RESET_ALL,
                hostname,
            ))


def parse_cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "pattern", metavar="PATTERN", help="the regex pattern to search for")

    parser.add_argument(
        "-Q", "--literal", action="store_true", default=False,
        help="treat PATTERN as literal string, not a regex")

    parser.add_argument(
        "-d", "--debug", action="store_true", default=False,
        help="enable debugging output")

    parser.add_argument(
        "-s", "--skip", type=int, default=0,
        help="skip this many hostnames from the start")

    parser.add_argument(
        "-l", "--limit", type=int, default=None,
        help="stop after this many hostnames")

    parser.add_argument(
        "-n", dest='num_crawlers', type=int, default=20, help=(
            "how many processes to use in parallel (default: %(default)s)"))

    parser.add_argument(
        "-t", "--timeout", metavar='SECONDS', type=float, default=3.4, help=(
            "wait this many seconds to connect "
            "and again to read before timing out "
            "(default: %(default)s)"))

    return parser.parse_args()


if __name__ == '__main__':
    colorama.init()

    cli_args = parse_cli_args()

    pattern = cli_args.pattern
    if cli_args.literal:
        pattern = re.escape(cli_args.pattern)
    regex = re.compile(pattern, re.IGNORECASE)

    if cli_args.debug:
        import logging
        import http

        http.client.HTTPConnection.debuglevel = 1

        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

    log = Logger().log
    url_queue = Queue()

    with open('top-1m.csv') as csvfile:
        reader = csv.reader(csvfile)

        for row in reader:
            count = int(row[0])

            if cli_args.skip >= count:
                continue

            url_queue.put(row[1])

            if cli_args.limit:
                if count - cli_args.skip == cli_args.limit:
                    break

    crawlers = []
    for i in range(cli_args.num_crawlers):
        crawler = Process(
            target=Crawler,
            kwargs={
                'log': log,
                'regex': regex,
                'timeout': (
                    cli_args.timeout, # connect timeout
                    cli_args.timeout # read timeout
                ),
                'url_queue': url_queue,
            }
        )
        crawler.start()
        crawlers.append(crawler)

    # wait for all processes to finish
    for crawler in crawlers:
        crawler.join()
