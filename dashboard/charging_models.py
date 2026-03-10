# models/charging_models.py
from sqlalchemy import Column, Integer, String, DateTime, Date, DECIMAL, Text, JSON, ForeignKey, Index, Boolean
from sqlalchemy.orm import relationship
from database import Base
from datetime import datetime

class ChargingDataVersion(Base):
    __tablename__ = 'charging_data_versions'
    
    id = Column(Integer, primary_key=True, index=True)
    version_date = Column(Date, nullable=False)
    upload_time = Column(DateTime(6), nullable=False)
    description = Column(Text)
    
    # 关联关系
    station_daily_data = relationship("ChargingStationDaily", back_populates="version", cascade="all, delete-orphan")
    penetration_daily_data = relationship("ChargingPenetrationDaily", back_populates="version", cascade="all, delete-orphan")
    target_data = relationship("ChargingTarget", back_populates="version", cascade="all, delete-orphan")
    aggregated_station_data = relationship("ChargingAggregatedStation", back_populates="version", cascade="all, delete-orphan")
    aggregated_penetration_data = relationship("ChargingAggregatedPenetration", back_populates="version", cascade="all, delete-orphan")
    filter_cache = relationship("ChargingFilterCache", back_populates="version", cascade="all, delete-orphan")

class ChargingStationDaily(Base):
    __tablename__ = 'charging_station_data_daily'
    
    id = Column(Integer, primary_key=True, index=True)
    version_id = Column(Integer, ForeignKey('charging_data_versions.id', ondelete='CASCADE'), nullable=False)
    stat_date = Column(Date, nullable=False, index=True)
    station_id = Column(String(100), nullable=False, index=True)
    region = Column(String(50), nullable=False)
    province = Column(String(50), nullable=False)
    city = Column(String(50), nullable=False)
    station_type = Column(String(50))
    station_age = Column(String(20))
    gun_count = Column(Integer, default=0)
    service_fee_after_share = Column(DECIMAL(15, 2), default=0)
    card_sales_revenue = Column(DECIMAL(15, 2), default=0)
    parking_fee_revenue = Column(DECIMAL(15, 2), default=0)
    coupon_discount = Column(DECIMAL(15, 2), default=0)
    owner_discount = Column(DECIMAL(15, 2), default=0)
    card_discount = Column(DECIMAL(15, 2), default=0)
    charging_volume = Column(DECIMAL(15, 2), default=0)
    service_fee_before_share = Column(DECIMAL(15, 2), default=0)
    
    # 关联关系
    version = relationship("ChargingDataVersion", back_populates="station_daily_data")
    
    # 复合索引
    __table_args__ = (
        Index('idx_version_region_date', 'version_id', 'region', 'stat_date'),
    )

class ChargingPenetrationDaily(Base):
    __tablename__ = 'charging_penetration_data_daily'
    
    id = Column(Integer, primary_key=True, index=True)
    version_id = Column(Integer, ForeignKey('charging_data_versions.id', ondelete='CASCADE'), nullable=False)
    stat_date = Column(Date, nullable=False, index=True)
    region = Column(String(50), nullable=False)
    province = Column(String(50), nullable=False)
    city = Column(String(50), nullable=False)
    car_model = Column(String(50), nullable=False, index=True)
    self_charge_volume = Column(DECIMAL(15, 2), default=0)
    public_charge_volume = Column(DECIMAL(15, 2), default=0)
    
    # 关联关系
    version = relationship("ChargingDataVersion", back_populates="penetration_daily_data")
    
    # 复合索引
    __table_args__ = (
        Index('idx_version_region_date', 'version_id', 'region', 'stat_date'),
    )

class ChargingTarget(Base):
    __tablename__ = 'charging_target_data'
    
    id = Column(Integer, primary_key=True, index=True)
    version_id = Column(Integer, ForeignKey('charging_data_versions.id', ondelete='CASCADE'), nullable=False)
    region = Column(String(50), nullable=False)
    metric_name = Column(String(100), nullable=False)
    target_period = Column(String(20), nullable=False)
    target_value = Column(DECIMAL(15, 2), nullable=False)
    
    # 关联关系
    version = relationship("ChargingDataVersion", back_populates="target_data")
    
    # 复合索引
    __table_args__ = (
        Index('idx_version_region', 'version_id', 'region'),
    )

class ChargingAggregatedStation(Base):
    __tablename__ = 'charging_aggregated_station_data'
    
    id = Column(Integer, primary_key=True, index=True)
    version_id = Column(Integer, ForeignKey('charging_data_versions.id', ondelete='CASCADE'), nullable=False)
    aggregation_type = Column(String(20), nullable=False)  # monthly, weekly, quarterly
    period_key = Column(String(20), nullable=False, index=True)
    stat_date = Column(Date, nullable=False, index=True)
    region = Column(String(50), nullable=False)
    province = Column(String(50), nullable=False)
    city = Column(String(50), nullable=False)
    station_type = Column(String(50))
    station_age = Column(String(20))
    station_count = Column(Integer, default=0)
    total_guns = Column(Integer, default=0)
    gun_days = Column(Integer, default=0)
    service_fee_after_share = Column(DECIMAL(15, 2), default=0)
    card_sales_revenue = Column(DECIMAL(15, 2), default=0)
    parking_fee_revenue = Column(DECIMAL(15, 2), default=0)
    coupon_discount = Column(DECIMAL(15, 2), default=0)
    owner_discount = Column(DECIMAL(15, 2), default=0)
    card_discount = Column(DECIMAL(15, 2), default=0)
    charging_volume = Column(DECIMAL(15, 2), default=0)
    service_fee_before_share = Column(DECIMAL(15, 2), default=0)
    
    # 关联关系
    version = relationship("ChargingDataVersion", back_populates="aggregated_station_data")
    
    # 复合索引
    __table_args__ = (
        Index('idx_query', 'version_id', 'region', 'aggregation_type', 'stat_date'),
    )

class ChargingAggregatedPenetration(Base):
    __tablename__ = 'charging_aggregated_penetration_data'
    
    id = Column(Integer, primary_key=True, index=True)
    version_id = Column(Integer, ForeignKey('charging_data_versions.id', ondelete='CASCADE'), nullable=False)
    aggregation_type = Column(String(20), nullable=False)
    period_key = Column(String(20), nullable=False, index=True)
    stat_date = Column(Date, nullable=False, index=True)
    region = Column(String(50), nullable=False)
    province = Column(String(50), nullable=False)
    city = Column(String(50), nullable=False)
    car_model = Column(String(50), nullable=False, index=True)
    self_charge_volume = Column(DECIMAL(15, 2), default=0)
    public_charge_volume = Column(DECIMAL(15, 2), default=0)
    
    # 关联关系
    version = relationship("ChargingDataVersion", back_populates="aggregated_penetration_data")
    
    # 复合索引
    __table_args__ = (
        Index('idx_query', 'version_id', 'region', 'aggregation_type', 'stat_date'),
    )

class ChargingFilterCache(Base):
    __tablename__ = 'charging_filter_options_cache'
    
    id = Column(Integer, primary_key=True, index=True)
    version_id = Column(Integer, ForeignKey('charging_data_versions.id', ondelete='CASCADE'), nullable=False)
    region = Column(String(50), nullable=False)
    option_type = Column(String(50), nullable=False)
    option_values = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 关联关系
    version = relationship("ChargingDataVersion", back_populates="filter_cache")
    
    # 唯一约束
    __table_args__ = (
        Index('uk_version_region_type', 'version_id', 'region', 'option_type', unique=True),
    )