import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import numpy as np


import random
import string

def generate_random_name():
    vowels = 'aeiou'
    consonants = ''.join([c for c in string.ascii_lowercase if c not in vowels])
    name_length = random.randint(3, 10)
    name = ''.join(random.choice(consonants) + random.choice(vowels) for _ in range(name_length)).capitalize()
    return name

def generate_random_text():
    text_length = random.randint(20, 40)
    text = ''.join(random.choice(string.ascii_letters) for _ in range(text_length))
    return text

def generate_random_data(num_entries):
    data = []
    for _ in range(num_entries):
        entry = {
            'name': generate_random_name(),
            'text': generate_random_text()
        }
        data.append(entry)
    return data

def run_twitter_etl():
    num_entries = 5
    random_data = generate_random_data(num_entries)
    data = pd.DataFrame(random_data)
    data.to_csv("s3://ahmad-airflow-project/random_data.csv", index=False)

def run_twitter_etl_1():
    num_entries = 5
    random_data = generate_random_data(num_entries)
    data = pd.DataFrame(random_data)
    data.to_csv("s3://ahmad-airflow-project/random_data1.csv", index=False)

def run_twitter_etl_2():
    num_entries = 5
    random_data = generate_random_data(num_entries)
    data = pd.DataFrame(random_data)
    data.to_csv("s3://ahmad-airflow-project/random_data2.csv", index=False)

def run_twitter_etl_3():
    num_entries = 5
    random_data = generate_random_data(num_entries)
    data = pd.DataFrame(random_data)
    data.to_csv("s3://ahmad-airflow-project/random_data3.csv", index=False)

def run_twitter_etl_4():
    num_entries = 5
    random_data = generate_random_data(num_entries)
    data = pd.DataFrame(random_data)
    data.to_csv("s3://ahmad-airflow-project/random_data4.csv", index=False)
