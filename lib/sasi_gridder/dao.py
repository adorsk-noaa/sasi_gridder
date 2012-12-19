from sa_dao.geo_orm_dao import GeoORM_DAO
from sasi_gridder import models as models
from sqlalchemy import (Table, Column, ForeignKey, ForeignKeyConstraint, 
                        Integer, String, Text, Float, PickleType, 
                        create_engine, MetaData)
from sqlalchemy.orm import (mapper, relationship)
from geoalchemy import (GeometryExtensionColumn, MultiPolygon, 
                        GeometryColumn, GeometryDDL)
from geoalchemy import functions as geo_funcs
import sys
import logging


class SASIGridderDAO(GeoORM_DAO):

    valid_funcs = GeoORM_DAO.valid_funcs + [
        'func.ST_Contains',
        'func.ST_GeomFromText',
        'func.BuildCircleMbr',
    ]

    def __init__(self, session=None, create_tables=True, **kwargs):
        self.session = session
        self.setUp()
        GeoORM_DAO.__init__(self, session=self.session, schema=self.schema,
                         **kwargs)
        self.valid_funcs.append('func.st_intersects')
        self.valid_funcs.append('geo_funcs.intersects')
        self.valid_funcs.append('geo_funcs._within_distance')
        self.expression_locals['geo_funcs'] = geo_funcs
        if create_tables:
            self.create_tables()

    def setUp(self):
        self.metadata = MetaData()
        self.schema = self.generateSchema()

    def get_local_mapped_class(self, base_class, table, local_name, **kw):
        local_class = type(local_name, (base_class,), {})
        mapper(local_class, table, **kw)
        return local_class

    def create_tables(self, bind=None):
        if not bind:
            bind = self.session.bind
        self.metadata.create_all(bind=bind)

    def generateSchema(self):
        schema = { 'sources': {} }

        # Define tables and mappings.
        mappings = {}

        # Cell.
        mappings['Cell'] = {
            'table': Table('cell', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('area', Float),
                           # Note: if upgrade to SA 8,
                           # mutable types have changed...watch out.
                           Column('keyed_values', PickleType(mutable=True)),
                           GeometryExtensionColumn('geom', MultiPolygon(2)),
                          ),
            'is_spatial': True,
        }
        mappings['Cell']['mapper_kwargs'] = {
            'properties': {
                'geom': GeometryColumn(mappings['Cell']['table'].c.geom)
            }
        }

        # StatArea.
        mappings['StatArea'] = {
            'table': Table('statarea', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('keyed_values', PickleType(mutable=True)),
                           GeometryExtensionColumn('geom', MultiPolygon(2)),
                          ),
            'is_spatial': True,
        }
        mappings['StatArea']['mapper_kwargs'] = {
            'properties': {
                'geom': GeometryColumn(mappings['StatArea']['table'].c.geom)
            }
        }

        # Effort.
        mappings['Effort'] = {
            'table': Table('effort', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('cell_id', Integer),
                           Column('stat_area_id', Integer),
                           Column('gear_id', String),
                           Column('a', Float),
                           Column('hours_fished', Float),
                           Column('value', Float),
                           Column('time', Integer),
                           Column('lat', Float),
                           Column('lon', Float),
                          ),
        }

        for class_name, mapping in mappings.items():
            if mapping.get('is_spatial'):
                GeometryDDL(mapping['table'])
            mapped_class = self.get_local_mapped_class(
                getattr(models, class_name),
                mapping['table'],
                class_name,
                **mapping.get('mapper_kwargs', {})
            )
            schema['sources'][class_name] = mapped_class

        return schema
