import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
from pathlib import Path
import hashlib

from .config import Config
from .ais_decoder import AISDecoder
from .adsb_processor import ADSBProcessor
from .models import AISData, ADSData, ResourceCoverage, DataEncoder, DataCleaningStats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
    """数据处理器 - 集成多个AIS文件和多个ADS-B文件数据处理"""

    def __init__(self):
        self.config = Config()
        self.ais_decoder = AISDecoder()
        self.adsb_processor = ADSBProcessor()
        self.processed_data = None
        self.data_hash = None
        self.cleaning_stats = DataCleaningStats()

    def process_all_data(self, force_update: bool = False) -> Dict[str, Any]:
        """
        处理所有数据，返回标准化格式
        """
        # 检查缓存是否存在且有效
        if not force_update:
            cache_data = self._load_cached_data()
            if cache_data is not None:
                logger.info(f"使用缓存的已处理数据")
                logger.info(
                    f"缓存数据统计: AIS={cache_data.get('metadata', {}).get('ais_count', 0)}, ADS-B={cache_data.get('metadata', {}).get('adsb_count', 0)}")
                return cache_data

        logger.info("开始处理所有数据...")

        # 重置清洗统计
        self.cleaning_stats = DataCleaningStats()

        # 1. 处理所有AIS数据文件
        logger.info("处理AIS数据文件...")
        all_ais_data = self._process_all_ais_files()

        # 2. 处理所有ADS-B数据文件
        logger.info("处理ADS-B数据文件...")
        all_adsb_data = self._process_all_adsb_files()

        # 3. 创建资源覆盖范围
        logger.info("创建资源覆盖范围...")
        coverage_layers = self._create_coverage_layers(all_ais_data, all_adsb_data)

        # 4. 标准化数据格式
        logger.info("标准化数据格式...")
        standardized_data = self._standardize_data(all_ais_data, all_adsb_data, coverage_layers)

        # 5. 保存到缓存
        self._save_to_cache(standardized_data)

        self.processed_data = standardized_data
        logger.info(f"数据处理完成。AIS: {len(all_ais_data)}条, ADS-B: {len(all_adsb_data)}条")

        return standardized_data

    def _process_all_ais_files(self) -> List[AISData]:
        """处理所有AIS文件"""
        all_ais_data = []
        ais_files = self.config.get_ais_files()

        if not ais_files:
            logger.warning("未找到任何AIS文件")
            return all_ais_data

        logger.info(f"找到 {len(ais_files)} 个AIS文件")

        for ais_file in ais_files:
            try:
                logger.info(f"处理AIS文件: {ais_file.name}")
                file_data = self.ais_decoder.decode_ais_file(str(ais_file))
                all_ais_data.extend(file_data)

                # 收集清洗统计
                file_stats = self.ais_decoder.get_cleaning_stats()
                self._merge_cleaning_stats(file_stats)

                logger.info(f"文件 {ais_file.name} 处理完成，获得 {len(file_data)} 条记录")
            except Exception as e:
                logger.error(f"处理AIS文件 {ais_file.name} 时出错: {str(e)}")
                continue

        logger.info(f"AIS数据总共处理完成，共 {len(all_ais_data)} 条有效位置记录")
        return all_ais_data

    def _process_all_adsb_files(self) -> List[ADSData]:
        """处理所有ADS-B文件"""
        all_adsb_data = []
        adsb_files = self.config.get_adsb_files()

        if not adsb_files:
            logger.warning("未找到任何ADS-B文件")
            return all_adsb_data

        logger.info(f"找到 {len(adsb_files)} 个ADS-B文件")

        for adsb_file in adsb_files:
            try:
                logger.info(f"处理ADS-B文件: {adsb_file.name}")
                file_data = self.adsb_processor.process_adsb_file(str(adsb_file))
                all_adsb_data.extend(file_data)

                # 收集清洗统计
                file_stats = self.adsb_processor.get_cleaning_stats()
                self._merge_cleaning_stats(file_stats)

                logger.info(f"文件 {adsb_file.name} 处理完成，获得 {len(file_data)} 条记录")
            except Exception as e:
                logger.error(f"处理ADS-B文件 {adsb_file.name} 时出错: {str(e)}")
                continue

        logger.info(f"ADS-B数据总共处理完成，共 {len(all_adsb_data)} 条有效位置记录")
        return all_adsb_data

    def _merge_cleaning_stats(self, file_stats: Dict[str, Any]):
        """合并清洗统计"""
        self.cleaning_stats.total_records += file_stats.get('total_records', 0)
        self.cleaning_stats.valid_records += file_stats.get('valid_records', 0)
        self.cleaning_stats.error_records += file_stats.get('error_records', 0)
        self.cleaning_stats.warning_records += file_stats.get('warning_records', 0)

        # 合并错误类型
        for error_type, count in file_stats.get('errors_by_type', {}).items():
            self.cleaning_stats.errors_by_type[error_type] = \
                self.cleaning_stats.errors_by_type.get(error_type, 0) + count

        # 合并警告类型
        for warning_type, count in file_stats.get('warnings_by_type', {}).items():
            self.cleaning_stats.warnings_by_type[warning_type] = \
                self.cleaning_stats.warnings_by_type.get(warning_type, 0) + count

    def _create_coverage_layers(self, ais_data: List[AISData], adsb_data: List[ADSData]) -> List[Dict[str, Any]]:
        """创建资源覆盖范围图层"""
        coverage_layers = []

        # AIS覆盖范围
        if ais_data:
            ais_coverage = self._calculate_coverage_area([(d.longitude, d.latitude) for d in ais_data])
            ais_layer = ResourceCoverage(
                resource_id="ais_coverage_layer",
                data_type="ais",
                coordinates=ais_coverage,
                status="online",
                label="AIS Coverage Area",
                metadata={
                    "data_count": len(ais_data),
                    "update_time": datetime.now().isoformat(),
                    "description": "船舶自动识别系统覆盖区域",
                    "data_sources": [str(file) for file in self.config.get_ais_files()]
                }
            )
            coverage_layers.append(ais_layer.to_dict())

        # ADS-B覆盖范围
        if adsb_data:
            adsb_coverage = self._calculate_coverage_area([(d.longitude, d.latitude) for d in adsb_data])
            adsb_layer = ResourceCoverage(
                resource_id="adsb_coverage_layer",
                data_type="adsb",
                coordinates=adsb_coverage,
                status="online",
                label="ADS-B Coverage Area",
                metadata={
                    "data_count": len(adsb_data),
                    "update_time": datetime.now().isoformat(),
                    "description": "广播式自动相关监视覆盖区域",
                    "data_sources": [str(file) for file in self.config.get_adsb_files()]
                }
            )
            coverage_layers.append(adsb_layer.to_dict())

        return coverage_layers

    def _calculate_coverage_area(self, coordinates: List[tuple]) -> List[List[float]]:
        """计算覆盖区域（简化版本：使用凸包或边界矩形）"""
        if not coordinates:
            return []

        # 过滤无效坐标
        valid_coords = []
        for lon, lat in coordinates:
            if -180 <= lon <= 180 and -90 <= lat <= 90:
                valid_coords.append((lon, lat))

        if not valid_coords:
            return []

        # 计算边界矩形
        lons = [c[0] for c in valid_coords]
        lats = [c[1] for c in valid_coords]

        min_lon, max_lon = min(lons), max(lons)
        min_lat, max_lat = min(lats), max(lats)

        # 扩展边界，使覆盖区域更明显
        lon_margin = max((max_lon - min_lon) * 0.1, 0.01)
        lat_margin = max((max_lat - min_lat) * 0.1, 0.01)

        min_lon -= lon_margin
        max_lon += lon_margin
        min_lat -= lat_margin
        max_lat += lat_margin

        # 确保边界在有效范围内
        min_lon = max(min_lon, -180)
        max_lon = min(max_lon, 180)
        min_lat = max(min_lat, -90)
        max_lat = min(max_lat, 90)

        # 创建矩形区域
        return [
            [min_lon, min_lat],
            [max_lon, min_lat],
            [max_lon, max_lat],
            [min_lon, max_lat],
            [min_lon, min_lat]  # 闭合多边形
        ]

    def _standardize_data(self, ais_data: List[AISData], adsb_data: List[ADSData],
                          coverage_layers: List[Dict]) -> Dict[str, Any]:
        """标准化数据格式，包含数据质量统计"""
        # 分离不同状态的数据
        normal_ais_data = [ais for ais in ais_data if ais.data_status == "normal"]
        warning_ais_data = [ais for ais in ais_data if ais.data_status == "warning"]
        error_ais_data = [ais for ais in ais_data if ais.data_status == "error"]

        normal_adsb_data = [adsb for adsb in adsb_data if adsb.data_status == "normal"]
        warning_adsb_data = [adsb for adsb in adsb_data if adsb.data_status == "warning"]
        error_adsb_data = [adsb for adsb in adsb_data if adsb.data_status == "error"]

        # 统计不同数据状态
        ais_status_stats = {
            "normal": len(normal_ais_data),
            "warning": len(warning_ais_data),
            "error": len(error_ais_data)
        }

        adsb_status_stats = {
            "normal": len(normal_adsb_data),
            "warning": len(warning_adsb_data),
            "error": len(error_adsb_data)
        }

        # 分离不同格式的数据
        nmea_ais_data = []
        csv_ais_data = []

        for ais in ais_data:
            # 根据来源判断格式
            if hasattr(ais, 'vessel_name') and ais.vessel_name != 'unknown':
                csv_ais_data.append(ais)
            else:
                nmea_ais_data.append(ais)

        # 分离不同格式的ADS-B数据
        jsonl_adsb_data = []
        csv_adsb_data = []

        for adsb in adsb_data:
            # 根据是否有heading_deg判断格式（CSV格式通常没有航向信息）
            if adsb.heading_deg == 0.0 and adsb.data_type == "adsb":
                csv_adsb_data.append(adsb)
            else:
                jsonl_adsb_data.append(adsb)

        standardized = {
            "metadata": {
                "version": "2.3",
                "total_records": len(ais_data) + len(adsb_data),
                "ais_count": len(ais_data),
                "ais_by_format": {
                    "nmea": len(nmea_ais_data),
                    "csv": len(csv_ais_data)
                },
                "ais_by_status": ais_status_stats,
                "adsb_count": len(adsb_data),
                "adsb_by_format": {
                    "jsonl": len(jsonl_adsb_data),
                    "csv": len(csv_adsb_data)
                },
                "adsb_by_status": adsb_status_stats,
                "processing_time": datetime.now().isoformat(),
                "coordinate_system": "WGS-84",
                "data_sources": {
                    "ais_files": [str(file) for file in self.config.get_ais_files()],
                    "adsb_files": [str(file) for file in self.config.get_adsb_files()]
                },
                "data_quality": {
                    "ais_normal_percentage": f"{(len(normal_ais_data) / max(len(ais_data), 1) * 100):.1f}%",
                    "ais_warning_percentage": f"{(len(warning_ais_data) / max(len(ais_data), 1) * 100):.1f}%",
                    "ais_error_percentage": f"{(len(error_ais_data) / max(len(ais_data), 1) * 100):.1f}%",
                    "adsb_normal_percentage": f"{(len(normal_adsb_data) / max(len(adsb_data), 1) * 100):.1f}%",
                    "adsb_warning_percentage": f"{(len(warning_adsb_data) / max(len(adsb_data), 1) * 100):.1f}%",
                    "adsb_error_percentage": f"{(len(error_adsb_data) / max(len(adsb_data), 1) * 100):.1f}%"
                },
                "data_cleaning": self.cleaning_stats.to_dict(),
                "file_status": {
                    "ais_nmea_exists": self.config.AIS_NMEA_FILE.exists(),
                    "ais_csv_exists": self.config.AIS_CSV_FILE.exists(),
                    "adsb_jsonl_exists": self.config.ADSB_JSONL_FILE.exists(),
                    "adsb_csv_exists": self.config.ADSB_CSV_FILE.exists()
                }
            },
            "ais_data": [ais.to_dict() for ais in ais_data],
            "adsb_data": [adsb.to_dict() for adsb in adsb_data],
            "coverage_layers": coverage_layers,
            "status_summary": {
                "online_ais": len([ais for ais in ais_data if self._is_online(ais)]),
                "offline_ais": len([ais for ais in ais_data if not self._is_online(ais)]),
                "online_adsb": len([adsb for adsb in adsb_data if self._is_online(adsb)]),
                "offline_adsb": len([adsb for adsb in adsb_data if not self._is_online(adsb)])
            }
        }

        return standardized

    def _is_online(self, data_point) -> bool:
        """判断数据点是否在线（简化版本）"""
        # 实际项目中应根据时间戳判断，这里使用简单逻辑
        if hasattr(data_point, 'timestamp'):
            try:
                time_diff = datetime.now() - data_point.timestamp
                return time_diff.total_seconds() < self.config.MAX_DATA_AGE_HOURS * 3600
            except:
                return True
        return True

    def _save_to_cache(self, data: Dict[str, Any]):
        """保存处理后的数据到缓存"""
        try:
            # 确保缓存目录存在
            self.config.CACHE_DIR.mkdir(parents=True, exist_ok=True)

            # 创建临时文件路径
            temp_file = self.config.PROCESSED_DATA_CACHE.with_suffix('.tmp')

            # 写入临时文件
            with open(temp_file, 'w', encoding='utf-8') as f:
                # 使用自定义编码器
                json_str = json.dumps(data, cls=DataEncoder, ensure_ascii=False, indent=2)
                f.write(json_str)

            # 验证写入的数据
            with open(temp_file, 'r', encoding='utf-8') as f:
                test_data = json.load(f)
                if 'metadata' not in test_data or 'ais_data' not in test_data:
                    raise ValueError("缓存数据格式不正确")

            # 重命名为正式文件
            temp_file.replace(self.config.PROCESSED_DATA_CACHE)

            file_size = self.config.PROCESSED_DATA_CACHE.stat().st_size
            logger.info(f"数据已缓存到: {self.config.PROCESSED_DATA_CACHE} (大小: {file_size} 字节)")

        except Exception as e:
            logger.error(f"保存缓存时出错: {str(e)}")
            # 清理临时文件
            if 'temp_file' in locals() and temp_file.exists():
                temp_file.unlink(missing_ok=True)

    def _load_cached_data(self) -> Dict[str, Any]:
        """从缓存加载数据"""
        if not self.config.PROCESSED_DATA_CACHE.exists():
            logger.info("缓存文件不存在")
            return None

        try:
            # 检查文件大小
            file_size = self.config.PROCESSED_DATA_CACHE.stat().st_size
            if file_size == 0:
                logger.warning("缓存文件为空")
                return None

            with open(self.config.PROCESSED_DATA_CACHE, 'r', encoding='utf-8') as f:
                # 读取文件内容
                content = f.read()
                if not content.strip():
                    logger.warning("缓存文件内容为空")
                    return None

                # 解析JSON
                data = json.loads(content)

            # 验证数据格式
            if not isinstance(data, dict):
                logger.warning("缓存数据不是有效的JSON对象")
                return None

            if 'metadata' not in data:
                logger.warning("缓存数据缺少metadata字段")
                return None

            if 'ais_data' not in data or 'adsb_data' not in data:
                logger.warning("缓存数据缺少数据字段")
                return None

            logger.info(f"成功加载缓存数据，大小: {file_size} 字节")
            logger.info(f"缓存数据统计: AIS={len(data.get('ais_data', []))}, ADS-B={len(data.get('adsb_data', []))}")

            return data

        except json.JSONDecodeError as e:
            logger.warning(f"缓存文件JSON格式错误: {str(e)}")
            # 尝试修复JSON或创建备份
            self._backup_corrupted_cache()
            return None
        except Exception as e:
            logger.error(f"加载缓存时出错: {str(e)}")
            return None

    def _backup_corrupted_cache(self):
        """备份损坏的缓存文件"""
        try:
            if self.config.PROCESSED_DATA_CACHE.exists():
                backup_file = self.config.PROCESSED_DATA_CACHE.with_suffix('.bak')
                self.config.PROCESSED_DATA_CACHE.rename(backup_file)
                logger.info(f"已备份损坏的缓存文件: {backup_file}")
        except Exception as e:
            logger.error(f"备份缓存文件时出错: {str(e)}")

    def calculate_data_hash(self) -> str:
        """计算数据文件的哈希值，用于检测变化"""
        hash_md5 = hashlib.md5()

        # 添加所有AIS文件哈希
        for ais_file in self.config.get_ais_files():
            if ais_file.exists():
                with open(ais_file, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)

        # 添加所有ADS-B文件哈希
        for adsb_file in self.config.get_adsb_files():
            if adsb_file.exists():
                with open(adsb_file, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)

        return hash_md5.hexdigest()