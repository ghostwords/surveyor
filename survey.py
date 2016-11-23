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
from time import sleep

import colorama
import requests

from babel.dates import format_timedelta
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
        self.result_queue = kwargs['result_queue']

        while not self.url_queue.empty():
            try:
                hostname = self.url_queue.get()
                self.check(hostname)
            except KeyboardInterrupt:
                pass

    def fetch(self, url):
        #self.log("Fetching %s ..." % url)
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
            requests.exceptions.ConnectionError,
        ):
            pass
        except (
            requests.exceptions.ChunkedEncodingError,
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
        result = {
            'match': False,
            'error': False,
        }

        html = self.get_html(hostname)

        if html:
            if self.regex.search(html):
                result['match'] = True
                self.log("%s!!! Got a match on %s%s" % (
                    colorama.Back.GREEN + colorama.Fore.BLACK,
                    hostname,
                    colorama.Style.RESET_ALL,
                ))
        else:
            result['error'] = True
            self.log("%sXXX%s Failed to fetch %s" % (
                colorama.Fore.RED + colorama.Style.BRIGHT,
                colorama.Style.RESET_ALL,
                hostname,
            ))

        self.result_queue.put(result)


def collect(log, result_queue, start_time):
    counts = {
        'num_urls': 0,
        'num_matches': 0,
        'num_errors': 0,
    }

    while True:
        try:
            if result_queue.empty():
                sleep(0.01)
                continue

            result = result_queue.get()

            # time to stop collecting
            if result is None:
                break

            counts['num_urls'] += 1

            if result['match']:
                counts['num_matches'] += 1
            elif result['error']:
                counts['num_errors'] += 1

        except KeyboardInterrupt:
            pass

    print_summary(log, datetime.now() - start_time, **counts)


def enable_debug_output():
    import logging
    import http

    http.client.HTTPConnection.debuglevel = 1

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


def populate_url_queue(url_queue, skip, limit):
    with open('top-1m.csv') as csvfile:
        for row in csv.reader(csvfile):
            count = int(row[0])

            if skip >= count:
                continue

            url_queue.put(row[1])

            if limit:
                if count - skip == limit:
                    break


def print_summary(log, crawl_timedelta, num_urls, num_matches, num_errors):
    log("Searched %s URLs in %s (%.1f URLs/min)" % (
        num_urls,
        format_timedelta(crawl_timedelta),
        num_urls / crawl_timedelta.total_seconds() * 60,
    ))
    log("Got %s matches and %s failures" % (num_matches, num_errors))
    match_rate = (
        "%.1f%%" % (num_matches / (num_urls - num_errors) * 100)
        if num_urls > num_errors
        else "n/a"
    )
    log("Match rate: %s; failure rate: %.1f%%" % (
        match_rate,
        num_errors / num_urls * 100,
    ))


def parse_cli_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "pattern", metavar="PATTERN", help="the regex pattern to search for")

    parser.add_argument(
        "-Q", "--literal", action="store_true", default=False,
        help="treat PATTERN as literal string, not a regex")

    parser.add_argument(
        "-s", "--skip", type=int, default=0,
        help="skip this many hostnames from the start")

    parser.add_argument(
        "-l", "--limit", type=int, default=None,
        help="stop after this many hostnames")

    parser.add_argument(
        "-n", dest='num_processes', type=int, default=20,
        help="use this many processes in parallel (default: %(default)s)")

    parser.add_argument(
        "-t", "--timeout", metavar='SECONDS', type=float, default=3.4, help=(
            "wait this many seconds to connect "
            "and again to read before timing out "
            "(default: %(default)s)"))

    parser.add_argument(
        "-d", "--debug", action="store_true", default=False,
        help="enable debugging output")

    return parser.parse_args()


if __name__ == '__main__':
    colorama.init()

    cli_args = parse_cli_args()

    pattern = cli_args.pattern
    if cli_args.literal:
        pattern = re.escape(cli_args.pattern)
    regex = re.compile(pattern, re.IGNORECASE)

    if cli_args.debug:
        enable_debug_output()

    log = Logger().log
    url_queue = Queue()
    result_queue = Queue()

    populate_url_queue(url_queue, cli_args.skip, cli_args.limit)

    start_time = datetime.now()

    crawlers = []
    for i in range(cli_args.num_processes):
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
                'result_queue': result_queue,
            }
        )
        crawler.start()
        crawlers.append(crawler)

    # start the collector process
    Process(target=collect, args=(log, result_queue, start_time)).start()

    try:
        # wait for all processes to finish
        for crawler in crawlers:
            crawler.join()
    except KeyboardInterrupt:
        # handle early termination
        for crawler in crawlers:
            crawler.terminate()
            crawler.join()
        print()
    finally:
        # tell collector we are done
        result_queue.put(None)

    log("All done.")
