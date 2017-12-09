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
from datetime import datetime
import argparse
import os
import json

from tqdm import tqdm
import grequests
import pandas as pd
import requests

# SCORE_ZERO_WORDS = ['uncle', 'people', 'little', 'puppy', 'cousin', 'aunt', 'parents', 'but']


args = argparse.ArgumentParser()
args.add_argument('-s', '--start', type=int, default=1, help="start row number")
args.add_argument('-d', '--directory', default='downloads', help="download directory")
args.add_argument('-n', '--nThread', type=int, default=50, help="group requests size")
args.add_argument('-f', '--file', default='', help='data file')
args.add_argument('-e', '--sheet', type=int, default=0, help="sheet to read")
args.add_argument('-l', '--list', nargs='*', type=str, help="specify words to download")
FLAGS = args.parse_args()


def download_amr(word_name, url):
    save_dir = os.path.join(FLAGS.directory, word_name)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    response = requests.get(url)
    filename = os.path.join(save_dir, url.split('/')[-1])
    with open(filename, 'w') as f:
        f.write(response.content)
        print('within word %s, %s saved!' % (word_name, filename))


def group_download_amr(words_name, urls):
    """ download amr file using grequests
    words and urls corresponded by same index
    :param words_name: a sequence of word
    :param urls: a sequence of url
    :return: tuple, no response url count, downloads count
    """
    rs = (grequests.get(u) for u in urls)
    response_set = grequests.map(rs)
    no_response_cnt, download_cnt = 0, 0
    for word_name, response in zip(words_name, response_set):

        if not response:
            no_response_cnt += 1
            continue
        if response.status_code != 200:
            # print('url: %s, bad response code: %s. skipped!'
            # % (response.url, response.status_code))
            continue
        if not response.content:
            # print('url: %s, no content, skipped!' % response.url)
            continue

        save_dir = os.path.join(FLAGS.directory, word_name)
        filename = os.path.join(save_dir, response.url.split('/')[-1])

        if not os.path.exists(save_dir):
            os.makedirs(save_dir)

        with open(filename, 'w') as f:
            f.write(response.content)
            download_cnt += 1
    return no_response_cnt, download_cnt


def main():
    t_start = datetime.now()
    xls = pd.ExcelFile(FLAGS.file)
    df = xls.parse(FLAGS.sheet)

    t_download_start = datetime.now()
    print('load data completed: %s' % (t_download_start - t_start))

    data_to_process = df.iloc[FLAGS.start - 1:, -1].values
    all_len = len(data_to_process)
    words_set, urls_set = [], []
    no_response_cnt, download_cnt = 0, 0
    with tqdm(data_to_process,
              postfix={'null': no_response_cnt,
                       'down': download_cnt}) as t:
        for idx, data in enumerate(t, 1):

            json_str = data
            data_dict = json.loads(json_str)
            if not data_dict.get('question') or len(data_dict.get('question').split()) != 1:
                continue

            url = data_dict.get('mp3Url')
            filename = url.split('/')[-1]
            word_name = data_dict.get('question')

            if FLAGS.list and word_name not in FLAGS.list:
                continue

            if os.path.exists(os.path.join(FLAGS.directory, word_name, filename)):
                continue

            if len(words_set) == FLAGS.nThread or idx == all_len:
                null_cnt, dwn_cnt = group_download_amr(words_set, urls_set)
                no_response_cnt += null_cnt
                download_cnt += dwn_cnt
                t.set_postfix({'null': no_response_cnt,
                               'down': download_cnt})
                words_set, urls_set = [], []
            else:
                words_set.append(word_name)
                urls_set.append(url)

    print('start time: %s,\ncompleted time:%s' % (t_start, datetime.now()))


if __name__ == '__main__':
    main()
