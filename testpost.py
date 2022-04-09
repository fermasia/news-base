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

#----------------------------------------------------------------
# SqlAlchemy Only
#----------------------------------------------------------------
from sqlalchemy import create_engine


# Here you want to change your database, username & password according to your own values
param_dic = {
    "host"      : "localhost",
    "database"  : "fmasia",
    "user"      : "fmasia",
    "password"  : "password"
}

connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    param_dic['user'],
    param_dic['password'],
    param_dic['host'],
    param_dic['database']
)

def to_alchemy(df):
    """
    Using a dummy table to test this call library
    """
    engine = create_engine(connect)
    df.to_sql(
        'test_table', 
        con=engine, 
        index=False, 
        if_exists='replace'
    )
    print("to_sql() done (sqlalchemy)")

def read_table(table):
    engine = create_engine(connect)
    table_df = pd.read_sql_table(table,con=engine)
    print(table_df.head(10)
)


read_table('news')



#conn = connect(param_dic)
#param_dic = {
#    "host"      : "localhost",
#    "database"  : "fmasia",
#    "user"      : "fmasia",
#    "password"  : "password"
#}

#connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
#    param_dic['user'],
#    param_dic['password'],
#    param_dic['host'],
#    param_dic['database']
#)

#engine = create_engine(connect)

#df_table = Table('news', meta,
#                 Column('source', text),
#                 Column('category', text),
#                 Column('pubdate', timestamp),
#                 Column('title', text),
#                 Column('description', text),
#                 Column('link', text)
#                )

#insert_statement = postgresql.insert(df_table).values(df.to_dict(orient='records'))
#upsert_statement = insert_statement.on_conflict_do_update(
#    index_elements=['id'],
#    set_={c.key: c for c in insert_statement.excluded if c.key != 'id'})
#conn.execute(upsert_statement)


