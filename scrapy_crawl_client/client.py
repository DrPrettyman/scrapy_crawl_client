import json
import os
import io
import yaml
import subprocess
import datetime
import uuid
import re

from typing import List, Union
from urllib.parse import urlparse

from . import crawlprocpy_path

default_process_settings = {
    "HTTPCACHE_ENABLED": False,
    "ROBOTSTXT_OBEY": True,
    "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
    "DEFAULT_REQUEST_HEADERS": {
        # we should use headers
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en',
    }
}


class Client:
    def __init__(self, cache_path: str):
        self.cache_path = cache_path

    @property
    def crawls(self):
        crawl_dirs = os.listdir(self.cache_path)
        _process_dicts = []
        for _dir in crawl_dirs:
            _yaml_file = os.path.join(_dir, 'process.yaml')
            if not os.path.exists(_yaml_file):
                continue
            with open(_yaml_file, 'r') as stream:
                _d = yaml.safe_load(stream)
            _process_dicts.append(_d)
        return _process_dicts

    def get_crawls(self, domain: str):
        return 0

    def spider_process(self,
                       url: Union[str, List[str]],
                       name: str = None,
                       allowed_sub_domains: List[str] = None,
                       crawl_limit: int = None,
                       process_settings: dict = None):

        if not process_settings:
            process_settings = default_process_settings

        if isinstance(url, str):
            urls = [url]
        else:
            urls = url

        _domains = [get_domain(url=_u) for _u in urls]
        domain = _domains[0]

        if not all(_d == domain for _d in _domains):
            raise ValueError("list of urls are not all from the same domain")

        if not name:
            name = get_domain_text(urls[0])

        p = SpiderProcess()
        p.build(
            cache_path=self.cache_path,
            process_settings=process_settings,
            domain=domain,
            name=name,
            start_urls=urls,
            sub_domains=allowed_sub_domains,
            crawl_limit=crawl_limit
        )

        return p


class SpiderProcess:
    def __init__(self):
        self._bool: bool = False
        self.timestamp: datetime.datetime = None
        self.process_settings: dict = None
        self.name: str = None
        self.domain: str = None
        self.start_urls: List[str] = None
        self.uuid_namespace: uuid.UUID = None
        self.allowed_domains: List[str] = None
        self.crawl_limit: int = None

        self.cache_path: str = None
        self.html_cache_path: str = None
        self.process_yaml: str = None
        self.scrapy_log: str = None
        self.progress_log: str = None
        self.crawl_csv: str = None

    def from_yaml(self, path: str):
        with open(path, 'r') as stream:
            process_dict = yaml.safe_load(stream)

        self.timestamp = datetime.datetime.strptime(process_dict.get('timestamp'), '%Y-%m-%d %H:%M:%S.%f')
        self.process_settings = process_dict.get('process_settings')
        self.name = process_dict.get('name')
        self.domain = process_dict.get('domain')
        self.start_urls = process_dict.get('start_urls')
        self.uuid_namespace = uuid.UUID(process_dict.get('uuid_namespace'))
        self.allowed_domains = process_dict.get('allowed_domains')
        self.crawl_limit = process_dict.get('crawl_limit')

        self.cache_path = process_dict.get('cache_path')

        self.html_cache_path = os.path.join(self.cache_path, 'html')

        os.makedirs(self.cache_path, exist_ok=True)
        os.makedirs(self.html_cache_path, exist_ok=True)

        self.process_yaml = os.path.join(self.cache_path, 'process.yaml')
        self.scrapy_log = os.path.join(self.cache_path, 'scrapy.log')
        self.progress_log = os.path.join(self.cache_path, 'progress.log')
        self.crawl_csv = os.path.join(self.cache_path, 'crawl.csv')

        if not os.path.exists(self.crawl_csv):
            with open(self.crawl_csv, 'w') as _f:
                _f.write("uuid, url, status, download_latency, ip_address, protocol, timestamp\n")

        with open(self.progress_log, 'a') as _f:
            _f.write(f"process_init_from_yaml: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')}")

        self._bool = True

    def build(self,
              cache_path: str,
              process_settings: dict,
              domain: str,
              start_urls: List[str],
              name: str,
              sub_domains: List[str] = None,
              crawl_limit: int = None):

        self.timestamp = datetime.datetime.now()

        if sub_domains:
            _allowed_domains = []
            for _sd in sub_domains:
                if _sd[0] != '/':
                    _sd = '/' + _sd
                _allowed_domains.append(domain + _sd)
        else:
            _allowed_domains = [domain]

        self.process_settings = process_settings
        self.name = name
        self.domain = domain
        self.start_urls = start_urls
        self.uuid_namespace = uuid.uuid3(namespace=uuid.NAMESPACE_DNS, name=self.domain)
        self.allowed_domains = _allowed_domains
        self.crawl_limit = crawl_limit

        _time_stamp_format = self.timestamp.strftime("%Y%m%d-%H%M%S-%f")

        self.cache_path = os.path.join(cache_path, self.uuid_namespace.hex + '_' + _time_stamp_format)
        self.html_cache_path = os.path.join(self.cache_path, 'html')

        os.makedirs(self.cache_path, exist_ok=True)
        os.makedirs(self.html_cache_path, exist_ok=True)

        self.process_yaml = os.path.join(self.cache_path, 'process.yaml')
        self.scrapy_log = os.path.join(self.cache_path, 'scrapy.log')
        self.progress_log = os.path.join(self.cache_path, 'progress.log')
        self.crawl_csv = os.path.join(self.cache_path, 'crawl.csv')

        with open(self.crawl_csv, 'w') as _f:
            _f.write("uuid, url, status, download_latency, ip_address, protocol, timestamp\n")

        with open(self.progress_log, 'w') as _f:
            _f.write(f"process_init: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')}")

        self._bool = True

    def __dict__(self) -> dict:
        _d = {
            'timestamp': self.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'cache_path': self.cache_path,
            'name': self.name,
            'domain': self.domain,
            'start_urls': self.start_urls,
            'uuid_namespace': self.uuid_namespace.hex,
            'allowed_domains': self.allowed_domains,
            'crawl_limit': self.crawl_limit,
            'process_settings': self.process_settings,
        }
        return _d

    def __repr__(self) -> str:
        return json.dumps(self.__dict__(), indent=4)

    def __bool__(self):
        return self._bool

    def _write_yaml(self):
        with io.open(self.process_yaml, 'w', encoding='utf8') as outfile:
            yaml.dump(self.__dict__(), outfile, default_flow_style=False, allow_unicode=True)

    def _create_run_command(self):
        command_items = list()
        command_items.append('python3')
        command_items.append(crawlprocpy_path)
        command_items.append(f"--cache_path '{self.cache_path}'")
        return ' '.join(command_items)

    def run(self):
        self._write_yaml()
        bash_command = self._create_run_command()
        print(f"running command {bash_command}")
        _scrapy_log_feed = open(self.scrapy_log, 'a')
        result = subprocess.run(
            bash_command,
            shell=True,
            stdout=_scrapy_log_feed,
            stderr=subprocess.STDOUT,
            text=True)
        _scrapy_log_feed.close()
        return result


def get_domain(url: str) -> str:
    host_name = urlparse(url).hostname
    if m := re.match(r'(?:www.)?([\w.]+)', host_name):
        domain = m.group(1)
    else:
        raise ValueError(f"cannot extract domain from url {url}")
    return domain


def get_domain_text(url: str) -> str:
    host_name = urlparse(url).hostname
    if m := re.match(r'(?:www.)?(\w+)', host_name):
        text = m.group(1)
    else:
        text = re.sub(r'\W', '', host_name)
    return text
