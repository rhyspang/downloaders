# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:       downloader
   Description: 
   Author:          rhys
   date:            17/12/2
-------------------------------------------------
"""
from __future__ import print_function
import argparse
import json
import os
from datetime import datetime

import pandas as pd
import requests
import grequests


SCORE_ZERO_WORDS = ['uncle', 'people', 'little', 'puppy', 'cousin', 'aunt', 'parents', 'but']
DATA_FILENAME = 'data.xlsx'


args = argparse.ArgumentParser()
args.add_argument('-s', '--start', type=int, default=1, help="start row number")
args.add_argument('-d', '--directory', default='downloads', help="download directory")
args.add_argument('-n', '--nThread', type=int, default=50, help="group requests size")
args.add_argument('-i', '--interval', type=int, default=200, help="log interval")
opts = args.parse_args()


def download_amr(word_name, url):
    save_dir = os.path.join(opts.directory, word_name)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    response = requests.get(url)
    filename = os.path.join(save_dir, url.split('/')[-1])
    with open(filename, 'w') as f:
        f.write(response.content)
        print('within word %s, %s saved!' % (word_name, filename))


def group_download_amr(words_name, urls):
    rs = (grequests.get(u) for u in urls)
    response_set = grequests.map(rs)
    for word_name, response in zip(words_name, response_set):
        if not response:
            print('response is null, skipped!')
            continue
        if response.status_code != 200:
            print('url: %s, bad response code: %s. skipped!' % (response.url, response.status_code))
            continue
        if not response.content:
            print('url: %s, no content, skipped!' % response.url)
            continue
        save_dir = os.path.join(opts.directory, word_name)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        filename = os.path.join(save_dir, response.url.split('/')[-1])
        with open(filename, 'w') as f:
            f.write(response.content)
            # print('within word %s, %s saved!' % (word_name, filename))


def main():
    t_start = datetime.now()
    xls = pd.ExcelFile(DATA_FILENAME)
    df = xls.parse(0)

    t_download_start = datetime.now()
    print('load data completed: %s' % (t_download_start-t_start))

    data_to_process = df.iloc[opts.start-1:, -1].values
    all_len = len(data_to_process)
    words_set, urls_set = [], []
    for idx, data in enumerate(data_to_process, 1):
        if not idx % opts.interval:
            print('-'*50)
            print('%.2f%%  -- %s of %s completed!' %
                  (float(idx+opts.start)*100 / (all_len+opts.start),
                   idx+opts.start, all_len+opts.start))
            interval = datetime.now() - t_download_start
            print('time elapsed: %s' % interval)
            completed_time = all_len * interval / idx
            print('About %s before task completed!' % completed_time)

        json_str = data
        data_dict = json.loads(json_str)
        question_len = len(data_dict.get('question').split())
        if question_len != 1:
            continue
        url = data_dict.get('mp3Url')

        word_name = data_dict.get('question')

        if not idx % opts.nThread or idx == all_len-1:
            group_download_amr(words_set, urls_set)
            words_set, urls_set = [], []
        else:
            words_set.append(word_name)
            urls_set.append(url)

    print('start time: %s,\ncompleted time:%s' % (t_start, datetime.now()))


if __name__ == '__main__':
    main()
