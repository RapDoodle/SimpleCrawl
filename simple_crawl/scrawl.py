#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import re
import string
import datetime
import argparse
import requests
import itertools
import concurrent.futures
from random import gauss

from tqdm import tqdm
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from urllib.parse import quote, unquote, urljoin

from .task import Task
from .store import MongoDBStore

def _main():
    parser = argparse.ArgumentParser(description='Crawl from the web.')
    parser.add_argument(
        '-c',
        '--create', 
        required=False, 
        default=None,
        help='Create a new task')
    parser.add_argument(
        '-r',
        '--resume', 
        required=False, 
        default=None,
        help='Resume a task.')
    parser.add_argument(
        '--flush-interval',
        required=False,
        type=int,
        default=100,
        help='The interval between progress snapshot.'
    )
    parser.add_argument(
        '--connections',
        required=False,
        type=int,
        default=1,
        help='Number of concurrent connections.'
    )
    parser.add_argument(
        '--mongodb-url',
        required=False,
        type=str,
        default=os.getenv('MONGODB_URL', 'mongodb://127.0.0.1/'),
        help='Database access URL.'
    )
    args = parser.parse_args()

    if args.create is not None:
        # Create a task
        task = Task()
        task.init_from_template(args.create)
        print(f'[√] Created task {task.id} from template {args.create}')
    elif args.resume is not None:
        task = Task()
        task.load_progress(args.resume)
        print(f'[√] Loaded task {task.id} from {args.resume}')
    else:
        print(f'Unknown operation.')
        exit(1)

    # Setup MongoDB
    if args.mongodb_url is None and os.getenv('MONGODB_URL') is None:
        print('Variable MONGODB_URL is not specified as environment variable or command line arguments.')
        exit(1)
    store = MongoDBStore(args.mongodb_url)

    task_config = task.progress['config']
    crawler_opt = task_config.get('crawlerOptions', {})
    experimental_opt = task_config.get('experimentalOptions', {})

    # Prepare the access control list
    acl = task_config['acl']
    for i in range(len(acl)):
        acl[i] = (re.compile(acl[i][0]), acl[i][1])

    session = requests.Session()
    session.mount(
        'https://', 
        requests.adapters.HTTPAdapter(
            pool_connections=args.connections, 
            max_retries=requests.adapters.Retry(
                total=crawler_opt.get('maxRetry', 5), 
                backoff_factor=crawler_opt.get('backoffFactor', 0.1), 
                status_forcelist=[500, 502, 503, 504]
            )
        )
    )

    # Crawl with BFS
    queue = task.progress['queue']
    visited = task.progress['visited']
    failed = task.progress['failed']
    inserted = task.progress['inserted']

    def flush():
        store.commit()
        task.save_progress()

    def bfs_crawl():
        completed = itertools.count(len(visited))
        pbar = tqdm(total=len(queue)+len(visited))
        pbar.update(len(visited))

        def fetch(url):
            if url in visited:
                return
            try:
                response = session.get(url, headers=crawler_opt.get('headers', {}), allow_redirects=True, timeout=crawler_opt.get('timeout', 5))
            except (requests.RequestException, requests.Timeout, requests.ConnectionError) as e:
                pbar.write(f'Error occured while crawling {url}. Error: {str(e)}')
                failed.add(url)
                return
            
            content = response.content.decode()
            parsed_html = BeautifulSoup(content, 'html.parser')
            store.update(
                {'_id': url},
                {'$set': {
                    'url': unquote(url),
                    'statusCode': response.status_code,
                    'headers': response.headers,
                    'title': parsed_html.title.text if parsed_html.title is not None else '',
                    'content': content,
                    'fetchedAt': datetime.datetime.utcnow(),
                    'taskId': task.id
                }},
                upsert=True
            )
            if parsed_html.title is None:
                pbar.write(f'{url} does not have a title.')
            
            curr_completed = next(completed)
            
            if crawler_opt.get('recursive', False):
                visited.add(url)
                parsed_html = BeautifulSoup(content, 'html.parser')

                links = parsed_html.find_all('a', href=True)
                for link in links:
                    new_url = urljoin(url, link['href'])
                    for acl_entry in acl:
                        if re.match(acl_entry[0], new_url) and acl_entry[1]:
                            # The rule accepted the url
                            if (new_url not in visited) and (new_url not in inserted):
                                inserted.add(new_url)
                                queue.append(new_url)
                            break
            
            pbar.total = len(queue) + curr_completed
            pbar.refresh()
            pbar.update(1)
            
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.connections) as executor:
            future_list = []
            while queue:
                curr_count = args.flush_interval
                while queue and curr_count > 0:
                    url = quote(queue.popleft(), safe=string.printable)
                    future_list.append(executor.submit(fetch, url))
                    curr_count = curr_count - 1
                concurrent.futures.wait(future_list, timeout=60)
                flush()
        pbar.close()
    
    bfs_crawl()
    max_retry_from_failed = experimental_opt.get('failedUrlMaxRetry', 0)
    if len(failed) > 0 and max_retry_from_failed > 0:
        while max_retry_from_failed > 0:
            for failed_url in failed:
                queue.append(failed_url)
            failed.clear()
            bfs_crawl()
            max_retry_from_failed = max_retry_from_failed - 1
    flush()

if __name__ == '__main__':
    _main()

