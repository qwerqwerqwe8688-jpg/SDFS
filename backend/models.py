from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from datetime import datetime
import json


@dataclass
class AISData:
    """AIS数据模型"""
    mmsi: str
    latitude: float
    longitude: float
    sog: float  # 航速
    cog: float  # 航向
    heading: float
    nav_status: str
    vessel_type: str
    timestamp: datetime
    data_type: str = "ais"

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['timestamp'] = self.timestamp.isoformat()
        return result


@dataclass
class ADSData:
    """ADS-B数据模型"""
    aircraft_id: str
    latitude: float
    longitude: float
    altitude_ft: float
    ground_speed_kts: float
    heading_deg: float
    aircraft_tail: str
    timestamp: datetime
    data_type: str = "adsb"

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result['timestamp'] = self.timestamp.isoformat()
        return result


@dataclass
class ResourceCoverage:
    """资源覆盖范围模型"""
    resource_id: str
    data_type: str
    coordinates: list  # [[lon, lat], ...]
    status: str  # online/offline
    label: str
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'resource_id': self.resource_id,
            'data_type': self.data_type,
            'coordinates': self.coordinates,
            'status': self.status,
            'label': self.label,
            'metadata': self.metadata
        }


class DataEncoder(json.JSONEncoder):
    """自定义JSON编码器，用于处理datetime对象"""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)