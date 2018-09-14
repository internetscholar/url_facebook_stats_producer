import psycopg2
from psycopg2 import extras
import boto3
import json
import configparser
import os
import logging


def main():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    logging.info('Connected to database %s', config['database']['db_name'])

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=config['aws']['region_queues']
    )
    sqs = aws_session.resource('sqs')
    logging.info('Connected to AWS in %s', aws_credential['region_name'])

    # Connect to queue for Twitter credentials.
    credentials_queue = sqs.get_queue_by_name(QueueName='facebook_credentials')
    credentials_queue.purge()
    # Enqueue all Twitter credentials available on the database.
    cur.execute("""select * from facebook_user_credentials;""")
    credentials = cur.fetchall()
    for credential in credentials:
        credential_user = {
            'type': 'user',
            'credentials': [
                {
                    'id': credential['email'],
                    'access_token': credential['access_token']
                }
            ]
        }
        message_body = json.dumps(credential_user)
        credentials_queue.send_message(MessageBody=message_body)
    # todo : maybe break app credentials in groups with 3 credentials per message.
    credential_app = {
        'type': 'app',
        'credentials': []
    }
    cur.execute("""select * from facebook_app_credentials;""")
    credentials = cur.fetchall()
    for credential in credentials:
        credential_app['credentials'].append({
            'id': credential['app_name'],
            'access_token': "{}|{}".format(credential['app_id'], credential['app_secret'])
        })
    message_body = json.dumps(credential_app)
    credentials_queue.send_message(MessageBody=message_body)
    logging.info('Enqueued all facebook credentials')

    conn.close()

    ids_queue = sqs.get_queue_by_name(QueueName='url_facebook_stats')
    ids_queue.purge()
    logging.info('Purged queue for urls')

    with psycopg2.connect(host=config['database']['host'],
                          dbname=config['database']['db_name'],
                          user=config['database']['user'],
                          password=config['database']['password']) as conn_urls:
        with conn_urls.cursor(name="cursor_urls", cursor_factory=extras.RealDictCursor) as cursor_urls:
            logging.info('Will start SQL query')
            cursor_urls.execute("""
                select
                  url.project_name,
                  url.url
                from
                  url,
                  project
                where
                  url.project_name = project.project_name and
                  project.active and
                  url.url not like '%.facebook.com%' and
                  url.status_code <> 601 and
                  not exists(
                    select *
                    from
                      url_facebook_stats
                    where
                      url_facebook_stats.url = url.url and
                      url_facebook_stats.project_name = url.project_name
                    );    
            """)
            logging.info('Finished query')
            no_more_results = False
            while not no_more_results:
                urls = cursor_urls.fetchmany(size=100)
                if len(urls) == 0:
                    no_more_results = True
                    logging.info('No more results')
                else:
                    ids_queue.send_message(MessageBody=json.dumps(urls))
                    logging.info('Sent message with %d URLs', len(urls))

    logging.info('Disconnected from database')


if __name__ == '__main__':
    main()
