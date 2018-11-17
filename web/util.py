MYSQL_URL = 'wdias:wdias123@adapter-metadata-mysql.default.svc.cluster.local/metadata'
DB_ENGINES = {}


def get_engine(db_name):
    import sqlalchemy, os
    if db_name not in DB_ENGINES:
        DB_ENGINES[db_name] = sqlalchemy.create_engine('mysql+mysqlconnector://' + MYSQL_URL, pool_pre_ping=True, pool_size=4, pool_recycle=600)
    return DB_ENGINES[db_name]
