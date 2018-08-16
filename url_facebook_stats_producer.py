import psycopg2
from psycopg2 import extras
import boto3
import json
import configparser
import os

if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    cur.execute("""select * from aws_credentials;""")
    aws_credential = cur.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['region_name']
    )
    sqs = aws_session.resource('sqs')

    # Connect to queue for Twitter credentials.
    credentials_queue = sqs.get_queue_by_name(QueueName='facebook-credentials')
    credentials_queue.purge()
    # Enqueue all Twitter credentials available on the database.
    cur.execute("""select * from facebook_credentials;""")
    credentials = cur.fetchall()
    for credential in credentials:
        message_body = json.dumps(credential)
        credentials_queue.send_message(MessageBody=message_body)

    ids_queue = sqs.get_queue_by_name(QueueName='facebook-urls')
    ids_queue.purge()

    cur.execute("SELECT * FROM query WHERE status = 'FACEBOOK'")
    queries = cur.fetchall()
    for query in queries:
        cur.execute("""select url, min(created_at)::text as created_at, max(twitter) as twitter, max(google) as google
                        from
                        (select expanded_url as url,
                           min(created_at)::date as created_at,
                           1 as twitter,
                           0 as google
                        from tweet_url
                        where query_alias = '{0}' and position('.facebook.com' in expanded_url) = 0
                        group by expanded_url
                        union all
                        select url,
                          min(case when date is not NULL then date else initial_date end) as created_at,
                          0 as twitter, 1 as google
                        from search_result
                        where query_alias = '{0}' and position('facebook.com' in domain) = 0
                        group by url) subquery
                        where not exists (select *
                          from facebook_url_stats
                          where facebook_url_stats.query_alias = '{0}' and 
                          subquery.url = facebook_url_stats.url)
                        group by url;""".format(query['query_alias']))
        no_more_results = False
        while not no_more_results:
            ids = cur.fetchmany(size=100)
            if len(ids) == 0:
                no_more_results = True
            else:
                message_body = {
                    'query_alias': query['query_alias'],
                    'urls': [{'url': id['url'],
                              'created_at': id['created_at'],
                              'twitter': id['twitter'],
                              'google': id['google']} for id in ids]
                }
                ids_queue.send_message(MessageBody=json.dumps(message_body))

    conn.close()