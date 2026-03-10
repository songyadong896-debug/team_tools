from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


# 点位相关
class PointBase(BaseModel):
    city: str
    name: str
    gaode_name: Optional[str]
    lng: Optional[float]
    lat: Optional[float]
    matched: bool = False
    confidence: float = 0

class WaypointCreate(PointBase):
    order_num: int

class Waypoint(WaypointCreate):
    id: int
    route_id: int
    
    class Config:
        from_attributes = True

class AttractionCreate(PointBase):
    pass

class Attraction(AttractionCreate):
    id: int
    route_id: int
    
    class Config:
        from_attributes = True

class StationCreate(PointBase):
    pass

class Station(StationCreate):
    id: int
    route_id: int
    
    class Config:
        from_attributes = True

# 线路相关
class RouteBase(BaseModel):
    name: str
    color: str = '#002D28'

class RouteCreate(RouteBase):
    waypoints: List[WaypointCreate]
    attractions: List[AttractionCreate]
    stations: Optional[List[StationCreate]] = []

class RouteUpdate(BaseModel):
    name: Optional[str]
    color: Optional[str] = "#002D28"
    waypoints: Optional[List[WaypointCreate]]
    attractions: Optional[List[AttractionCreate]]
    stations: Optional[List[StationCreate]]
    path: Optional[List[List[float]]] = None
    segments: Optional[List[dict]] = None
    distance: Optional[float] = 0
    duration: Optional[int] = 0

class Route(RouteBase):
    id: int
    distance: float
    duration: int
    path: Optional[List[List[float]]]
    segments: Optional[List[dict]]
    created_at: datetime
    updated_at: datetime
    waypoints: List[Waypoint]
    attractions: List[Attraction]
    stations: List[Station]
    
    class Config:
        from_attributes = True

# 地图配置
class MapConfigUpdate(BaseModel):
    amap_key: str
    amap_security_code: Optional[str]

class MapConfig(MapConfigUpdate):
    id: int
    updated_at: datetime
    
    class Config:
        from_attributes = True