import argparse
import getpass
import configparser
import psycopg2
import uuid



parser = argparse.ArgumentParser(description='Export data from PostgreSQL to CSV')
# Fetch from config file
parser.add_argument('-c', '--config', help='path to config file,\
                    no need to specify any other arguments if this is specified')
#Fetch from command line
parser.add_argument('-n', '--hostname', help='hostname')
parser.add_argument('-p', '--port', help='port')
parser.add_argument('-d', '--dbname', help='database name')
parser.add_argument('-u', '--user', help='username')
parser.add_argument('-q', '--query', help='query')
args = parser.parse_args()


# Resolve DB Connection String
if args.config:
    config = configparser.ConfigParser()
    config.read(args.config)
    host, port, dbname, user, password = config['conn_string'].values()
    queries = config['queries'].values()
else:
    host = args.hostname if args.hostname else 'localhost'
    dbname = args.dbname if args.dbname else 'postgres'
    port = int(args.port) if args.port else 5432
    user = args.user if args.user else input('Username: ')
    password = getpass.getpass('Password: ')
    if args.query:
        queries = args.query.split(';')


# Connect to DB
db_conn = psycopg2.connect(host=host, port=port,\
    dbname=dbname, user=user, password=password)
db_cursor = db_conn.cursor()


# Fetch Queries

if not queries:
    queries = input('Query: ').split(';')


# Execute Queries and Write to CSV
output_dir = config['output']['dir']
for query in queries:
    filename = output_dir + str(uuid.uuid4()) + '.csv'
    csv_query = 'COPY ({}) TO {} WITH CSV HEADER'.format(query,filename)
    print(csv_query)
    db_cursor.execute(csv_query)
    db_conn.commit()

db_cursor.close()
db_conn.close()
