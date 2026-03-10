from sqlalchemy import Column, String, Text, DateTime, Boolean, JSON, DECIMAL
from sqlalchemy.dialects.mysql import INTEGER, TINYINT
from datetime import datetime
from database import Base

class Route(Base):
    __tablename__ = 'routes'
    
    id = Column(INTEGER(unsigned=True), primary_key=True, index=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    color = Column(String(20), default='#002D28')
    distance = Column(DECIMAL(20, 2), default=0)
    duration = Column(INTEGER, default=0)
    path = Column(JSON)
    segments = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Waypoint(Base):
    __tablename__ = 'waypoints'
    
    id = Column(INTEGER(unsigned=True), primary_key=True, index=True, autoincrement=True)
    route_id = Column(INTEGER, nullable=False, index=True)  # 普通int
    order_num = Column(INTEGER, nullable=False)
    city = Column(String(50))
    name = Column(String(100), nullable=False)
    gaode_name = Column(String(100))
    lng = Column(DECIMAL(20, 6))
    lat = Column(DECIMAL(20, 6))
    matched = Column(TINYINT(1), default=0)
    confidence = Column(DECIMAL(3, 2), default=0)

class Attraction(Base):
    __tablename__ = 'attractions'
    
    id = Column(INTEGER(unsigned=True), primary_key=True, index=True, autoincrement=True)
    route_id = Column(INTEGER, nullable=False, index=True)  # 普通int
    city = Column(String(50))
    name = Column(String(100), nullable=False)
    gaode_name = Column(String(100))
    lng = Column(DECIMAL(20, 6))
    lat = Column(DECIMAL(20, 6))
    matched = Column(TINYINT(1), default=0)
    confidence = Column(DECIMAL(3, 2), default=0)

class Station(Base):
    __tablename__ = 'stations'
    
    id = Column(INTEGER(unsigned=True), primary_key=True, index=True, autoincrement=True)
    route_id = Column(INTEGER, nullable=False, index=True)  # 普通int
    city = Column(String(50))
    name = Column(String(100), nullable=False)
    gaode_name = Column(String(100))
    lng = Column(DECIMAL(20, 6))
    lat = Column(DECIMAL(20, 6))
    matched = Column(TINYINT(1), default=0)
    confidence = Column(DECIMAL(3, 2), default=0)

class MapConfig(Base):
    __tablename__ = 'map_config'
    
    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    amap_key = Column(String(100))
    amap_security_code = Column(String(100))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)