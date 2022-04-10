import pandas as pd
from sqlalchemy import create_engine

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn

# Define connection parameters
param_dic = {
    "host"      : "localhost",
    "database"  : "fmasia",
    "user"      : "fmasia",
    "password"  : "password"
}

# Build Connection
connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    param_dic['user'],
    param_dic['password'],
    param_dic['host'],
    param_dic['database']
)

# Actually connect to Database
engine = create_engine(connect)

# Iterate files to prepare what to insert
files = ['202112','202201','202202','202203']

for f in files:
    links = pd.read_sql_table('news',con=engine)['link'].to_list()   # Read existing entries
    news_path = '/home/fmasia/news-base/files/' + f + '.csv.gz'
    df = pd.read_csv(news_path,compression='gzip',usecols=['source','category','date','title','text','link'])
    df = df[~df.link.isin(links)]
    if not df.empty:
        df.to_sql('news', con=engine, if_exists='append',index=False)
        print(len(df.link),'inserted rows from:',f,'in db news')
    del df

