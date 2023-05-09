#!/usr/bin/env python3
import time
import json
import asyncio
import aiohttp
from confluent_kafka import Producer
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_API_KEY = os.environ.get('KAFKA_API_KEY')
KAFKA_API_SECRET = os.environ.get('KAFKA_API_SECRET')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
TOPIC_NAME = 'custom_topic'
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET
}
producer = Producer(producer_conf)

API_URL_TEMPLATE = 'https://lichess.org/api/games/user/{}'
API_HEADERS = {'Accept': 'application/x-ndjson'}

NUMBER_OF_GAMES = '1000'
ENTITY_NAMES = [
{'name': 'penguingim1', 'params': {'rated': 'true', 'perfType': 'bullet', 'opening': 'true', 'max': NUMBER_OF_GAMES, 'pgnInJson': 'true'}},
{'name': 'penguingim1', 'params': {'rated': 'true', 'perfType': 'blitz', 'opening': 'true', 'max': NUMBER_OF_GAMES, 'pgnInJson': 'true'}},
{'name': 'penguingim1', 'params': {'rated': 'true', 'perfType': 'ultrabullet', 'opening': 'true', 'max': NUMBER_OF_GAMES, 'pgnInJson': 'true'}}]


REQUEST_INTERVAL_SECONDS = 21
fetched_game_ids = {entity['name']: set() for entity in ENTITY_NAMES}

def delivery_report(err, msg):
    if err is not None:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

async def fetch_data(session, url, headers, params):
    async with session.get(url, headers=headers, params=params) as response:
        if response.status == 200:
            return (await response.text()).splitlines()
        elif response.status == 429:
            await asyncio.sleep(21)
            return await fetch_data(session, url, headers, params)
        else:
            print(f"Error fetching data: {response.status}")
            return []

async def main():
    try:
        async with aiohttp.ClientSession() as session:
            start_time = time.time()
            while True:
                tasks = [asyncio.create_task(fetch_data(session, API_URL_TEMPLATE.format(entity['name']), API_HEADERS, entity['params'])) for entity in ENTITY_NAMES]
                results = await asyncio.gather(*tasks)

                for entity, games_data in zip(ENTITY_NAMES, results):
                    entity_name = entity['name']
                    for game_data in games_data:
                        game_json = json.loads(game_data)
                        game_id = game_json['id']

                        if game_id not in fetched_game_ids[entity_name]:
                            fetched_game_ids[entity_name].add(game_id)
                            producer.produce(TOPIC_NAME, key=entity_name, value=game_data, callback=delivery_report)
                            producer.poll(30)

                producer.flush()
                time_elapsed = time.time() - start_time
                if time_elapsed >= 600: # 10 minutes in seconds
                    logging.info('The script has stopped after 10 minutes.')
                    return

                await asyncio.sleep(REQUEST_INTERVAL_SECONDS)
    except asyncio.TimeoutError:
        logging.info('The script has stopped after 10 minutes.')



asyncio.run(main())