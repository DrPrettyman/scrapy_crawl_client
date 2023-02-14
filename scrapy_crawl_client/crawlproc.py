import datetime
import os
import yaml
import uuid
import urllib.request
from typing import List

import argparse

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor


def crawl_process(cache_path):
    html_cache_path = os.path.join(cache_path, 'html')
    process_yaml = os.path.join(cache_path, 'process.yaml')
    progress_log = os.path.join(cache_path, 'progress.log')
    crawl_csv = os.path.join(cache_path, 'crawl.csv')

    with open(process_yaml, 'r') as stream:
        process_dict = yaml.safe_load(stream)

    _uuid_namespace = uuid.uuid3(namespace=uuid.NAMESPACE_DNS, name=process_dict.get('domain'))
    _external_ip: str = urllib.request.urlopen('https://ident.me').read().decode('utf8')
    _pid = os.getpid()

    with open(progress_log, 'a') as _f:
        _lines = [
            f"process_pid: {_pid}",
            f"external_ip: {_external_ip}"
            f"crawl_start: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}"
        ]
        _f.writelines(_lines)

    process = CrawlerProcess(
        settings=process_dict.get('process_settings', {})
    )

    process.crawl(
        CrawlSpider,
        start_urls=process_dict.get('start_urls'),
        name=process_dict.get('name'),
        uuid_namespace=_uuid_namespace,
        cache_path=cache_path,
        html_cache_path=html_cache_path,
        crawl_csv_file=crawl_csv,
        allowed_domains=process_dict.get('allowed_domains'),
        limit=process_dict.get('crawl_limit')
    )

    process.start(stop_after_crawl=True)

    with open(progress_log, 'a') as _f:
        _lines = [
            f"crawl_end: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}"
        ]
        _f.writelines(_lines)


class CrawlSpider(scrapy.spiders.CrawlSpider):
    name = 'spider'
    rules = (
        scrapy.spiders.Rule(
            LinkExtractor(),
            callback='save_html',
            follow=True,
            process_request='process_request'),
    )

    def __init__(self,
                 start_urls: List[str],
                 name: str,
                 uuid_namespace: uuid.UUID,
                 cache_path: str,
                 html_cache_path: str,
                 crawl_csv_file: str,
                 allowed_domains: List[str],
                 limit: int = None,
                 *arg,
                 **kwargs):

        super().__init__(*arg, **kwargs)

        self.name = name

        self.scraped_count: int = 0
        self.limit = limit
        self.start_urls = start_urls
        self.allowed_domains = allowed_domains

        self.uuid_namespace = uuid_namespace
        self.cache_path = cache_path
        self.html_cache_path = html_cache_path

        self.crawl_csv = crawl_csv_file

    def save_html(self, response):
        page_uuid_hex = uuid.uuid3(namespace=self.uuid_namespace, name=response.url).hex
        html_file_path = os.path.join(self.html_cache_path, page_uuid_hex + '.html')
        csv_line = "{0}, {1}, {2}, {3}, {4}, {5}, {6}\n".format(
            page_uuid_hex,
            response.url,
            response.status,
            response.meta.get('download_latency'),
            str(response.ip_address),
            response.protocol,
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        )
        with open(html_file_path, 'wb') as _f:
            _f.write(response.body)
        with open(self.crawl_csv, 'a') as _g:
            _g.write(csv_line)

    def process_request(self, request, response):
        if self.limit:
            if self.scraped_count < self.limit:
                self.scraped_count += 1
                return request
        else:
            return request


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--cache_path", type=str)
    args, unknown = parser.parse_known_args()
    crawl_process(cache_path=args.cache_path)
