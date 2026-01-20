from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, Any, List
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
    # CSV格式的扩展字段
    vessel_name: str = "unknown"
    imo: str = "unknown"
    call_sign: str = "unknown"
    status: str = "unknown"
    length: float = 0.0
    width: float = 0.0
    draft: float = 0.0
    cargo: str = "unknown"
    transceiver_class: str = "unknown"
    base_date_time: str = ""

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