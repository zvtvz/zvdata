# -*- coding: utf-8 -*-
import logging
import os
from typing import List

from sqlalchemy import create_engine
from sqlalchemy import schema
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import sessionmaker, Session

from zvdata.structs import EntityMixin

logger = logging.getLogger(__name__)

# provider_dbname -> engine
_db_engine_map = {}
# provider_dbname -> session
_db_session_map = {}

global_providers = []
global_entity_types = []

# provider -> [db_name1,db_name2...]
provider_map_dbnames = {
}

# db_name -> [declarative_base1,declarative_base2...]
dbname_map_base = {
}

# entity_type -> schema
entity_type_map_schema = {

}

context = {}


def init_context(data_path):
    context['data_path'] = data_path
    if not os.path.exists(data_path):
        os.makedirs(data_path)


def enum_value(x):
    return [e.value for e in x]


def get_db_name(data_schema):
    for db_name, base in dbname_map_base.items():
        if issubclass(data_schema, base):
            return db_name


def get_db_engine(provider: str,
                  db_name: str = None,
                  data_schema: object = None) -> Engine:
    if data_schema:
        db_name = get_db_name(data_schema=data_schema)

    db_path = os.path.join(context['data_path'], '{}_{}.db'.format(provider, db_name))

    engine_key = '{}_{}'.format(provider, db_name)
    db_engine = _db_engine_map.get(engine_key)
    if not db_engine:
        db_engine = create_engine('sqlite:///' + db_path, echo=False)
        _db_engine_map[engine_key] = db_engine
    return db_engine


def get_db_session(provider: str,
                   db_name: str = None,
                   data_schema: object = None) -> Session:
    return get_db_session_factory(provider, db_name, data_schema)()


def get_db_session_factory(provider: str,
                           db_name: str = None,
                           data_schema: object = None):
    if data_schema:
        db_name = get_db_name(data_schema=data_schema)

    session_key = '{}_{}'.format(provider, db_name)
    session = _db_session_map.get(session_key)
    if not session:
        session = sessionmaker()
        _db_session_map[session_key] = session
    return session


def register_schema(providers: List[str],
                    db_name: str,
                    schema_base: DeclarativeMeta,
                    entity_type: str = None):
    """
    decorator for schema

    :param providers: the supported providers for the schema
    :type providers:
    :param db_name: database name for the schema
    :type db_name:
    :param schema_base:
    :type schema_base:
    :param entity_type: only use for the Entity schema
    :type entity_type:
    :return:
    :rtype:
    """

    def register(cls):
        for provider in providers:
            # track in in  _providers
            if provider not in global_providers:
                global_providers.append(provider)

            if not provider_map_dbnames.get(provider):
                provider_map_dbnames[provider] = []
            provider_map_dbnames[provider].append(db_name)
            dbname_map_base[db_name] = schema_base

            # register the entity
            if issubclass(cls, EntityMixin):
                entity_type_ = entity_type
                if not entity_type:
                    entity_type_ = cls.__name__.lower()

                if entity_type_ not in global_entity_types:
                    global_entity_types.append(entity_type_)
                entity_type_map_schema[entity_type_] = cls

            # create the db & table
            engine = get_db_engine(provider, db_name=db_name)
            schema_base.metadata.create_all(engine)

            session_fac = get_db_session_factory(provider, db_name=db_name)
            session_fac.configure(bind=engine)

            # create index for 'timestamp','entity_id','code','report_period','updated_timestamp
            for table_name, table in iter(schema_base.metadata.tables.items()):
                index_list = []
                with engine.connect() as con:
                    rs = con.execute("PRAGMA INDEX_LIST('{}')".format(table_name))
                    for row in rs:
                        index_list.append(row[1])

                logger.debug('engine:{},table:{},index:{}'.format(engine, table_name, index_list))

                for col in ['timestamp', 'entity_id', 'code', 'report_period', 'updated_timestamp']:
                    if col in table.c:
                        column = eval('table.c.{}'.format(col))
                        index = schema.Index('{}_{}_index'.format(table_name, col), column)
                        if index.name not in index_list:
                            index.create(engine)
                for cols in [('timestamp', 'entity_id'), ('timestamp', 'code')]:
                    if (cols[0] in table.c) and (col[1] in table.c):
                        column0 = eval('table.c.{}'.format(col[0]))
                        column1 = eval('table.c.{}'.format(col[1]))
                        index = schema.Index('{}_{}_{}_index'.format(table_name, col[0], col[1]), column0, column1)
                        if index.name not in index_list:
                            index.create(engine)

        return cls

    return register
