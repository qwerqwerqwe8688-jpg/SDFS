import json
from datetime import datetime
from typing import List, Dict, Any
import logging
from .models import ADSData, DataCleaningStats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ADSBProcessor:
    """ADS-B数据处理器"""

    def __init__(self):
        self.processed_data = []
        self.cleaning_stats = DataCleaningStats()

    def process_adsb_file(self, file_path: str) -> List[ADSData]:
        """
        处理ADS-B JSONL文件中的所有数据，包含数据清洗
        """
        logger.info(f"开始处理ADS-B文件: {file_path}")

        # 重置清洗统计
        self.cleaning_stats = DataCleaningStats()

        processed_records = []

        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()

            total_lines = len(lines)
            logger.info(f"共读取到 {total_lines} 条ADS-B记录")

            valid_count = 0
            invalid_count = 0

            for i, line in enumerate(lines, 1):
                self.cleaning_stats.total_records += 1

                try:
                    # 移除空白字符
                    line = line.strip()
                    if not line:
                        invalid_count += 1
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['empty_line'] = \
                            self.cleaning_stats.errors_by_type.get('empty_line', 0) + 1
                        continue

                    # 解析JSON行
                    data = json.loads(line)

                    # 验证必要字段
                    if not all(key in data for key in ['latitude', 'longitude', 'aircraft_id']):
                        invalid_count += 1
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['missing_fields'] = \
                            self.cleaning_stats.errors_by_type.get('missing_fields', 0) + 1
                        logger.debug(f"第 {i} 行缺少必要字段")
                        continue

                    # 数据清洗检查
                    cleaning_notes = []
                    data_status = "normal"

                    # 检查核心字段
                    aircraft_id = str(data.get('aircraft_id', 'unknown'))
                    if not aircraft_id or aircraft_id == 'unknown':
                        cleaning_notes.append("航空器ID缺失")
                        data_status = "warning"
                        self.cleaning_stats.warning_records += 1
                        self.cleaning_stats.warnings_by_type['missing_aircraft_id'] = \
                            self.cleaning_stats.warnings_by_type.get('missing_aircraft_id', 0) + 1
                    else:
                        self.cleaning_stats.valid_records += 1

                    # 检查飞机尾号
                    aircraft_tail = data.get('aircraft_tail', 'unknown')
                    if not aircraft_tail or aircraft_tail == 'unknown':
                        cleaning_notes.append("飞机尾号缺失")
                        data_status = "warning" if data_status == "normal" else data_status

                    # 解析坐标并验证
                    try:
                        lat = float(data.get('latitude', 0.0))
                        lon = float(data.get('longitude', 0.0))
                    except (ValueError, TypeError):
                        invalid_count += 1
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['coordinate_format'] = \
                            self.cleaning_stats.errors_by_type.get('coordinate_format', 0) + 1
                        logger.debug(f"第 {i} 行坐标格式错误")
                        continue

                    # 验证坐标范围
                    if not (-90 <= lat <= 90):
                        cleaning_notes.append(f"纬度超出范围: {lat}")
                        data_status = "error"
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['latitude_range'] = \
                            self.cleaning_stats.errors_by_type.get('latitude_range', 0) + 1
                        invalid_count += 1
                        continue

                    if not (-180 <= lon <= 180):
                        cleaning_notes.append(f"经度超出范围: {lon}")
                        data_status = "error"
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['longitude_range'] = \
                            self.cleaning_stats.errors_by_type.get('longitude_range', 0) + 1
                        invalid_count += 1
                        continue

                    # 检查异常坐标（如0,0海洋交叉点）
                    if lat == 0 and lon == 0:
                        cleaning_notes.append("可疑坐标(0,0)")
                        data_status = "warning"
                        self.cleaning_stats.warning_records += 1
                        self.cleaning_stats.warnings_by_type['suspicious_coordinates'] = \
                            self.cleaning_stats.warnings_by_type.get('suspicious_coordinates', 0) + 1

                    # 解析高度并进行清洗
                    altitude_ft = float(data.get('altitude_ft', 0.0))

                    # 检查高度异常（负数或过高）
                    if altitude_ft < 0:
                        cleaning_notes.append(f"高度为负数: {altitude_ft}")
                        data_status = "error"
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['negative_altitude'] = \
                            self.cleaning_stats.errors_by_type.get('negative_altitude', 0) + 1
                        invalid_count += 1
                        continue

                    if altitude_ft > 60000:  # 假设商业飞机最大飞行高度
                        cleaning_notes.append(f"高度异常高: {altitude_ft}")
                        altitude_ft = min(altitude_ft, 60000)  # 截断到合理范围
                        data_status = "warning" if data_status == "normal" else data_status
                        self.cleaning_stats.warning_records += 1
                        self.cleaning_stats.warnings_by_type['high_altitude'] = \
                            self.cleaning_stats.warnings_by_type.get('high_altitude', 0) + 1

                    # 解析地速并进行清洗
                    ground_speed_kts = float(data.get('ground_speed_kts', 0.0))
                    if ground_speed_kts < 0 or ground_speed_kts > 1000:  # 假设合理速度范围
                        cleaning_notes.append(f"地速异常: {ground_speed_kts}")
                        ground_speed_kts = max(0, min(ground_speed_kts, 1000))
                        data_status = "warning" if data_status == "normal" else data_status
                        self.cleaning_stats.warning_records += 1
                        self.cleaning_stats.warnings_by_type['speed_range'] = \
                            self.cleaning_stats.warnings_by_type.get('speed_range', 0) + 1

                    # 解析航向并进行清洗
                    heading_deg = float(data.get('heading_deg', 0.0))
                    if heading_deg < 0 or heading_deg > 360:
                        cleaning_notes.append(f"航向异常: {heading_deg}")
                        heading_deg = heading_deg % 360  # 归一化到0-360度
                        data_status = "warning" if data_status == "normal" else data_status
                        self.cleaning_stats.warning_records += 1
                        self.cleaning_stats.warnings_by_type['heading_range'] = \
                            self.cleaning_stats.warnings_by_type.get('heading_range', 0) + 1

                    # 解析时间戳
                    timestamp = self._parse_timestamp(data)

                    # 创建ADS-B数据对象
                    adsb_data = ADSData(
                        aircraft_id=aircraft_id,
                        latitude=lat,
                        longitude=lon,
                        altitude_ft=altitude_ft,
                        ground_speed_kts=ground_speed_kts,
                        heading_deg=heading_deg,
                        aircraft_tail=aircraft_tail,
                        timestamp=timestamp,
                        data_status=data_status,
                        cleaning_notes="; ".join(cleaning_notes) if cleaning_notes else ""
                    )
                    processed_records.append(adsb_data)
                    valid_count += 1

                    # 每处理1000条记录输出一次进度
                    if i % 1000 == 0:
                        logger.info(f"已处理 {i}/{total_lines} 条ADS-B记录，有效: {valid_count}, 无效: {invalid_count}")

                except json.JSONDecodeError as e:
                    invalid_count += 1
                    self.cleaning_stats.error_records += 1
                    self.cleaning_stats.errors_by_type['json_decode'] = \
                        self.cleaning_stats.errors_by_type.get('json_decode', 0) + 1
                    logger.debug(f"解析第 {i} 行JSON时出错: {str(e)}")
                    continue
                except Exception as e:
                    invalid_count += 1
                    self.cleaning_stats.error_records += 1
                    self.cleaning_stats.errors_by_type['processing_error'] = \
                        self.cleaning_stats.errors_by_type.get('processing_error', 0) + 1
                    logger.debug(f"处理第 {i} 行时出错: {str(e)}")
                    continue

        except FileNotFoundError:
            logger.error(f"未找到ADS-B文件: {file_path}")
            return []
        except Exception as e:
            logger.error(f"读取ADS-B文件时出错: {str(e)}")
            return []

        # 输出清洗统计
        logger.info(f"ADS-B处理完成，有效记录: {valid_count}, 无效记录: {invalid_count}")
        logger.info(f"数据清洗统计: 正常={self.cleaning_stats.valid_records}, "
                    f"警告={self.cleaning_stats.warning_records}, "
                    f"错误={self.cleaning_stats.error_records}")

        return processed_records

    def _parse_timestamp(self, data: Dict[str, Any]) -> datetime:
        """解析时间戳"""
        try:
            year = data.get('year', 2023)
            month = data.get('month', 1)
            day = data.get('day', 1)
            hour = data.get('hour', 0)
            minute = data.get('minute', 0)
            second = data.get('second', 0)

            if isinstance(second, float):
                microsecond = int((second - int(second)) * 1000000)
                second = int(second)
            else:
                microsecond = 0

            return datetime(year, month, day, hour, minute, second, microsecond)
        except Exception as e:
            logger.warning(f"解析时间戳时出错，使用当前时间: {str(e)}")
            return datetime.now()

    def get_cleaning_stats(self) -> Dict[str, Any]:
        """获取数据清洗统计"""
        return self.cleaning_stats.to_dict()