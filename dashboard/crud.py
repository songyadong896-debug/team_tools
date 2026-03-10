from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, text
import models
import schemas
from typing import List, Optional
from datetime import datetime
from decimal import Decimal
import json
import logging

logger = logging.getLogger(__name__)

async def get_routes(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[dict]:
    try:
        # 获取所有路线
        result = await db.execute(
            select(models.Route)
            .offset(skip)
            .limit(limit)
        )
        routes = result.scalars().all()
        
        # 手动加载关联数据
        routes_data = []
        for route in routes:
            try:
                # 获取沿途点
                waypoints_result = await db.execute(
                    select(models.Waypoint)
                    .filter(models.Waypoint.route_id == int(route.id))
                    .order_by(models.Waypoint.order_num)
                )
                waypoints = waypoints_result.scalars().all()
                
                # 获取景点
                attractions_result = await db.execute(
                    select(models.Attraction)
                    .filter(models.Attraction.route_id == int(route.id))
                )
                attractions = attractions_result.scalars().all()
                
                # 获取场站
                stations_result = await db.execute(
                    select(models.Station)
                    .filter(models.Station.route_id == int(route.id))
                )
                stations = stations_result.scalars().all()
                
                # 组装数据
                route_dict = {
                    "id": int(route.id),
                    "name": route.name,
                    "color": route.color,
                    "distance": float(route.distance) if route.distance else 0,
                    "duration": route.duration or 0,
                    "path": route.path,
                    "segments": route.segments,
                    "created_at": route.created_at,
                    "updated_at": route.updated_at,
                    "waypoints": waypoints,
                    "attractions": attractions,
                    "stations": stations
                }
                routes_data.append(route_dict)
            except Exception as e:
                logger.error(f"Error loading route {route.id}: {e}")
                continue
        
        return routes_data
    except Exception as e:
        logger.error(f"Error in get_routes: {e}")
        raise

async def get_route(db: AsyncSession, route_id: int) -> Optional[dict]:
    try:
        # 获取路线
        result = await db.execute(
            select(models.Route).filter(models.Route.id == route_id)
        )
        route = result.scalar_one_or_none()
        
        if not route:
            return None
        
        # 手动加载关联数据
        waypoints_result = await db.execute(
            select(models.Waypoint)
            .filter(models.Waypoint.route_id == int(route_id))
            .order_by(models.Waypoint.order_num)
        )
        waypoints = waypoints_result.scalars().all()
        
        attractions_result = await db.execute(
            select(models.Attraction)
            .filter(models.Attraction.route_id == int(route_id))
        )
        attractions = attractions_result.scalars().all()
        
        stations_result = await db.execute(
            select(models.Station)
            .filter(models.Station.route_id == int(route_id))
        )
        stations = stations_result.scalars().all()
        
        return {
            "id": int(route.id),
            "name": route.name,
            "color": route.color,
            "distance": float(route.distance) if route.distance else 0,
            "duration": route.duration or 0,
            "path": route.path,
            "segments": route.segments,
            "created_at": route.created_at,
            "updated_at": route.updated_at,
            "waypoints": waypoints,
            "attractions": attractions,
            "stations": stations
        }
    except Exception as e:
        logger.error(f"Error in get_route: {e}")
        raise

async def create_route(db: AsyncSession, route: schemas.RouteCreate) -> dict:
    try:
        # 创建路线
        db_route = models.Route(
            name=route.name,
            color=route.color
        )
        db.add(db_route)
        await db.flush()  # 获取route.id
        
        logger.info(f"Created route with id: {db_route.id}")
        
        # 确保route_id是普通int（对于有符号int字段）
        route_id = int(db_route.id)
        
        # 添加沿途点
        for i, wp in enumerate(route.waypoints):
            try:
                db_waypoint = models.Waypoint(
                    route_id=route_id,
                    order_num=int(wp.order_num),
                    city=wp.city,
                    name=wp.name,
                    gaode_name=wp.gaode_name,
                    lng=Decimal(str(wp.lng)) if wp.lng is not None else None,
                    lat=Decimal(str(wp.lat)) if wp.lat is not None else None,
                    matched=bool(wp.matched),
                    confidence=Decimal(str(min(wp.confidence, 1.0)))  # 确保不超过1
                )
                db.add(db_waypoint)
            except Exception as e:
                logger.error(f"Error adding waypoint {i}: {e}")
                logger.error(f"Waypoint data: {wp}")
                raise
        
        # 添加景点
        for i, attr in enumerate(route.attractions):
            try:
                db_attraction = models.Attraction(
                    route_id=route_id,
                    city=attr.city,
                    name=attr.name,
                    gaode_name=attr.gaode_name,
                    lng=Decimal(str(attr.lng)) if attr.lng is not None else None,
                    lat=Decimal(str(attr.lat)) if attr.lat is not None else None,
                    matched=bool(attr.matched),
                    confidence=Decimal(str(min(attr.confidence, 1.0)))
                )
                db.add(db_attraction)
            except Exception as e:
                logger.error(f"Error adding attraction {i}: {e}")
                raise
        
        # 添加场站
        if route.stations:
            for i, station in enumerate(route.stations):
                try:
                    db_station = models.Station(
                        route_id=route_id,
                        city=station.city,
                        name=station.name,
                        gaode_name=station.gaode_name,
                        lng=Decimal(str(station.lng)) if station.lng is not None else None,
                        lat=Decimal(str(station.lat)) if station.lat is not None else None,
                        matched=bool(station.matched),
                        confidence=Decimal(str(min(station.confidence, 1.0)))
                    )
                    db.add(db_station)
                except Exception as e:
                    logger.error(f"Error adding station {i}: {e}")
                    raise
        
        await db.commit()
        
        # 返回创建的路线（包含关联数据）
        return await get_route(db, route_id)
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Error in create_route: {e}")
        import traceback
        traceback.print_exc()
        raise

async def update_route(
    db: AsyncSession, 
    route_id: int, 
    route: schemas.RouteUpdate
) -> Optional[dict]:
    try:
        # 获取路线
        result = await db.execute(
            select(models.Route).filter(models.Route.id == route_id)
        )
        db_route = result.scalar_one_or_none()
        
        if not db_route:
            return None
        
        # 更新基本信息
        if route.name is not None:
            db_route.name = route.name
        if route.color is not None:
            db_route.color = route.color
        
        # 处理大型JSON数据
        if route.path is not None:
            # 如果路径太大，可以考虑压缩或分批存储
            if len(str(route.path)) > 1000000:  # 1MB限制
                logger.warning(f"Route path is very large: {len(str(route.path))} chars")
            db_route.path = route.path
            
        if route.segments is not None:
            db_route.segments = route.segments
            
        if route.distance is not None:
            db_route.distance = Decimal(str(route.distance))
        if route.duration is not None:
            db_route.duration = int(route.duration)
        
        # 转换route_id为普通int
        route_id_int = int(route_id)
        
        # 更新沿途点
        if route.waypoints is not None:
            # 删除旧的沿途点
            await db.execute(
                delete(models.Waypoint).where(models.Waypoint.route_id == route_id_int)
            )
            
            # 添加新的沿途点
            for wp in route.waypoints:
                db_waypoint = models.Waypoint(
                    route_id=route_id_int,
                    order_num=int(wp.order_num),
                    city=wp.city,
                    name=wp.name,
                    gaode_name=wp.gaode_name,
                    lng=Decimal(str(wp.lng)) if wp.lng is not None else None,
                    lat=Decimal(str(wp.lat)) if wp.lat is not None else None,
                    matched=bool(wp.matched),
                    confidence=Decimal(str(min(wp.confidence, 1.0)))
                )
                db.add(db_waypoint)
        
        # 更新景点
        if route.attractions is not None:
            await db.execute(
                delete(models.Attraction).where(models.Attraction.route_id == route_id_int)
            )
            
            for attr in route.attractions:
                db_attraction = models.Attraction(
                    route_id=route_id_int,
                    city=attr.city,
                    name=attr.name,
                    gaode_name=attr.gaode_name,
                    lng=Decimal(str(attr.lng)) if attr.lng is not None else None,
                    lat=Decimal(str(attr.lat)) if attr.lat is not None else None,
                    matched=bool(attr.matched),
                    confidence=Decimal(str(min(attr.confidence, 1.0)))
                )
                db.add(db_attraction)
        
        # 更新场站
        if route.stations is not None:
            await db.execute(
                delete(models.Station).where(models.Station.route_id == route_id_int)
            )
            
            for station in route.stations:
                db_station = models.Station(
                    route_id=route_id_int,
                    city=station.city,
                    name=station.name,
                    gaode_name=station.gaode_name,
                    lng=Decimal(str(station.lng)) if station.lng is not None else None,
                    lat=Decimal(str(station.lat)) if station.lat is not None else None,
                    matched=bool(station.matched),
                    confidence=Decimal(str(min(station.confidence, 1.0)))
                )
                db.add(db_station)
        
        db_route.updated_at = datetime.utcnow()
        await db.commit()
        
        # 返回更新后的路线
        return await get_route(db, route_id_int)
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Error in update_route: {e}")
        import traceback
        traceback.print_exc()
        raise

async def delete_route(db: AsyncSession, route_id: int) -> bool:
    try:
        route_id_int = int(route_id)
        
        # 先删除关联数据
        await db.execute(
            delete(models.Waypoint).where(models.Waypoint.route_id == route_id_int)
        )
        await db.execute(
            delete(models.Attraction).where(models.Attraction.route_id == route_id_int)
        )
        await db.execute(
            delete(models.Station).where(models.Station.route_id == route_id_int)
        )
        
        # 删除路线
        result = await db.execute(
            delete(models.Route).where(models.Route.id == route_id)
        )
        
        await db.commit()
        return result.rowcount > 0
    except Exception as e:
        await db.rollback()
        logger.error(f"Error in delete_route: {e}")
        raise

async def get_map_config(db: AsyncSession) -> Optional[models.MapConfig]:
    result = await db.execute(select(models.MapConfig))
    return result.scalar_one_or_none()

async def update_map_config(
    db: AsyncSession, 
    config: schemas.MapConfigUpdate
) -> models.MapConfig:
    result = await db.execute(select(models.MapConfig))
    db_config = result.scalar_one_or_none()
    
    if not db_config:
        db_config = models.MapConfig(
            amap_key=config.amap_key,
            amap_security_code=config.amap_security_code
        )
        db.add(db_config)
    else:
        db_config.amap_key = config.amap_key
        db_config.amap_security_code = config.amap_security_code
        db_config.updated_at = datetime.utcnow()
    
    await db.commit()
    await db.refresh(db_config)
    return db_config