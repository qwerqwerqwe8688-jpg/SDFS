import json
from datetime import datetime
import logging
from typing import List, Dict, Any
import csv
from pathlib import Path
from .models import ADSData, DataCleaningStats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ADSBProcessor:
    """ADS-B数据处理器 - 支持JSONL和CSV格式"""

    def __init__(self):
        self.processed_data = []
        self.cleaning_stats = DataCleaningStats()

    def process_adsb_file(self, file_path: str) -> List[ADSData]:
        """
        处理ADS-B文件，支持JSONL和CSV格式
        """
        logger.info(f"开始处理ADS-B文件: {file_path}")

        # 重置清洗统计
        self.cleaning_stats = DataCleaningStats()

        file_path = Path(file_path)
        if not file_path.exists():
            logger.error(f"ADS-B文件不存在: {file_path}")
            return []

        # 检测文件格式
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()

            if self._is_jsonl_format(first_line):
                logger.info("检测到JSONL格式的ADS-B数据")
                return self._decode_jsonl_file(file_path)
            elif self._is_csv_format(first_line):
                logger.info("检测到CSV格式的ADS-B数据")
                return self._decode_csv_file(file_path)
            else:
                logger.warning(f"无法识别的ADS-B文件格式，第一行: {first_line[:100]}...")
                # 尝试JSONL格式解码
                return self._decode_jsonl_file(file_path)

        except Exception as e:
            logger.error(f"检测文件格式时出错: {str(e)}")
            return []

    def _is_csv_format(self, first_line: str) -> bool:
        """检查是否为CSV格式"""
        # 如果以JSON格式开头，肯定不是CSV
        if first_line.startswith('{') or first_line.startswith('['):
            return False

        # CSV格式通常包含逗号分隔的多个字段
        comma_count = first_line.count(',')

        # CSV格式的ADS-B数据通常包含这些列名
        csv_adsb_indicators = ['flight,', 'tail_number,', 'long,', 'lat,', 'alt,', 'manufacturer,', 'model,', 'squawk,',
                               'mph,', 'spotted,']

        # 检查是否包含多个逗号且包含ADS-B列名
        if comma_count > 5 and any(indicator in first_line for indicator in csv_adsb_indicators):
            return True

        return False

    def _is_jsonl_format(self, first_line: str) -> bool:
        """检查是否为JSONL格式"""
        # 宽松的JSONL检测：以大括号开头
        if not first_line.startswith('{'):
            return False

        # 尝试解析为JSON来验证格式
        try:
            data = json.loads(first_line)
            # 检查是否包含ADS-B关键字段
            if 'latitude' in data and 'longitude' in data:
                return True
        except json.JSONDecodeError:
            # 不是有效的JSON，尝试其他检测方法
            pass

        # 回退到字符串检测
        adsb_indicators = ['"latitude"', '"longitude"', '"altitude"', '"aircraft"', '"speed"']
        return any(indicator in first_line for indicator in adsb_indicators)

    def _decode_csv_file(self, file_path: Path) -> List[ADSData]:
        """解码CSV格式的ADS-B文件"""
        decoded_records = []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # 使用DictReader自动处理列名
                csv_reader = csv.DictReader(f)
                total_rows = 0
                valid_count = 0

                for row_num, row in enumerate(csv_reader, 1):
                    total_rows = row_num
                    self.cleaning_stats.total_records += 1

                    try:
                        # 解析必需字段
                        aircraft_id = row.get('flight', '').strip()
                        tail_number = row.get('tail_number', '').strip()
                        lon_str = row.get('long', '').strip()
                        lat_str = row.get('lat', '').strip()
                        alt_str = row.get('alt', '').strip()

                        # 数据清洗检查
                        cleaning_notes = []
                        data_status = "normal"

                        # 检查核心字段缺失
                        if not aircraft_id:
                            aircraft_id = tail_number if tail_number else "unknown"
                            if not aircraft_id or aircraft_id == 'unknown':
                                cleaning_notes.append("飞行标识缺失")
                                data_status = "warning"
                                self.cleaning_stats.warning_records += 1
                                self.cleaning_stats.warnings_by_type['missing_aircraft_id'] = \
                                    self.cleaning_stats.warnings_by_type.get('missing_aircraft_id', 0) + 1

                        if not tail_number or tail_number == 'unknown':
                            cleaning_notes.append("飞机尾号缺失")
                            data_status = "warning" if data_status == "normal" else data_status
                            self.cleaning_stats.warning_records += 1
                            self.cleaning_stats.warnings_by_type['missing_tail_number'] = \
                                self.cleaning_stats.warnings_by_type.get('missing_tail_number', 0) + 1

                        if not lon_str or not lat_str:
                            cleaning_notes.append("经纬度缺失")
                            data_status = "error"  # 没有位置数据，剔除
                            self.cleaning_stats.error_records += 1
                            self.cleaning_stats.errors_by_type['missing_coordinates'] = \
                                self.cleaning_stats.errors_by_type.get('missing_coordinates', 0) + 1
                            continue

                        # 转换数据类型并验证
                        try:
                            lon = float(lon_str)
                            lat = float(lat_str)
                        except ValueError:
                            cleaning_notes.append(f"坐标格式错误: LON={lon_str}, LAT={lat_str}")
                            data_status = "error"
                            self.cleaning_stats.error_records += 1
                            self.cleaning_stats.errors_by_type['coordinate_format'] = \
                                self.cleaning_stats.errors_by_type.get('coordinate_format', 0) + 1
                            continue

                        # 验证坐标范围
                        if not (-90 <= lat <= 90):
                            cleaning_notes.append(f"纬度超出范围: {lat}")
                            data_status = "error"
                            self.cleaning_stats.error_records += 1
                            self.cleaning_stats.errors_by_type['latitude_range'] = \
                                self.cleaning_stats.errors_by_type.get('latitude_range', 0) + 1
                            continue

                        if not (-180 <= lon <= 180):
                            cleaning_notes.append(f"经度超出范围: {lon}")
                            data_status = "error"
                            self.cleaning_stats.error_records += 1
                            self.cleaning_stats.errors_by_type['longitude_range'] = \
                                self.cleaning_stats.errors_by_type.get('longitude_range', 0) + 1
                            continue

                        # 检查异常坐标（如0,0海洋交叉点）
                        if lat == 0 and lon == 0:
                            cleaning_notes.append("可疑坐标(0,0)")
                            data_status = "warning" if data_status == "normal" else data_status
                            self.cleaning_stats.warnings_by_type['suspicious_coordinates'] = \
                                self.cleaning_stats.warnings_by_type.get('suspicious_coordinates', 0) + 1

                        # 解析高度并进行清洗
                        altitude_ft = 0.0
                        if alt_str:
                            try:
                                altitude_ft = float(alt_str)
                            except ValueError:
                                cleaning_notes.append(f"高度格式错误: {alt_str}")
                                altitude_ft = 0.0

                        # 检查高度异常（负数或过高）
                        if altitude_ft < 0:
                            cleaning_notes.append(f"高度为负数: {altitude_ft}")
                            data_status = "error"
                            self.cleaning_stats.error_records += 1
                            self.cleaning_stats.errors_by_type['negative_altitude'] = \
                                self.cleaning_stats.errors_by_type.get('negative_altitude', 0) + 1
                            continue

                        if altitude_ft > 60000:  # 假设商业飞机最大飞行高度
                            cleaning_notes.append(f"高度异常高: {altitude_ft}")
                            altitude_ft = min(altitude_ft, 60000)  # 截断到合理范围
                            data_status = "warning" if data_status == "normal" else data_status
                            self.cleaning_stats.warnings_by_type['high_altitude'] = \
                                self.cleaning_stats.warnings_by_type.get('high_altitude', 0) + 1

                        # 解析地速并进行清洗（mph转换为kts）
                        ground_speed_kts = 0.0
                        mph_str = row.get('mph', '').strip()
                        if mph_str:
                            try:
                                mph = float(mph_str)
                                ground_speed_kts = mph * 0.868976  # mph转换为kts
                            except ValueError:
                                cleaning_notes.append(f"速度格式错误: {mph_str}")

                        # 检查地速异常
                        if ground_speed_kts < 0 or ground_speed_kts > 1000:
                            cleaning_notes.append(f"地速异常: {ground_speed_kts} kts")
                            ground_speed_kts = max(0, min(ground_speed_kts, 1000))
                            data_status = "warning" if data_status == "normal" else data_status
                            self.cleaning_stats.warnings_by_type['speed_range'] = \
                                self.cleaning_stats.warnings_by_type.get('speed_range', 0) + 1

                        # 解析其他字段
                        squawk = row.get('squawk', '').strip()
                        manufacturer = row.get('manufacturer', '').strip()
                        model = row.get('model', '').strip()

                        # 解析时间戳
                        spotted_str = row.get('spotted', '').strip()
                        timestamp = datetime.now()  # 默认值

                        if spotted_str:
                            try:
                                # 尝试解析 "11/7/22 13:30" 格式
                                if '/' in spotted_str and ':' in spotted_str:
                                    # 分割日期和时间
                                    date_part, time_part = spotted_str.split(' ')
                                    month, day, year = date_part.split('/')
                                    hour, minute = time_part.split(':')

                                    # 处理年份（假设20xx年）
                                    year_int = int(year)
                                    if year_int < 100:
                                        year_int += 2000

                                    timestamp = datetime(year_int, int(month), int(day),
                                                         int(hour), int(minute))
                            except Exception:
                                cleaning_notes.append(f"时间戳格式错误: {spotted_str}")
                                data_status = "warning" if data_status == "normal" else data_status
                                self.cleaning_stats.warnings_by_type['timestamp_format'] = \
                                    self.cleaning_stats.warnings_by_type.get('timestamp_format', 0) + 1

                        # 创建ADS-B数据对象
                        adsb_data = ADSData(
                            aircraft_id=aircraft_id,
                            latitude=lat,
                            longitude=lon,
                            altitude_ft=altitude_ft,
                            ground_speed_kts=ground_speed_kts,
                            heading_deg=0.0,  # CSV格式没有航向信息
                            aircraft_tail=tail_number,
                            timestamp=timestamp,
                            data_type="adsb",
                            data_status=data_status,
                            cleaning_notes="; ".join(cleaning_notes) if cleaning_notes else ""
                        )

                        decoded_records.append(adsb_data)
                        valid_count += 1

                        # 更新统计
                        if data_status == "error":
                            self.cleaning_stats.error_records += 1
                        elif data_status == "warning":
                            self.cleaning_stats.warning_records += 1
                        else:
                            self.cleaning_stats.valid_records += 1

                        # 每处理1000条记录输出一次进度
                        if row_num % 1000 == 0:
                            logger.info(f"已处理 {row_num} 行CSV数据，有效: {valid_count}")

                    except Exception as e:
                        logger.debug(f"处理第 {row_num} 行CSV数据时出错: {str(e)}")
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['processing_error'] = \
                            self.cleaning_stats.errors_by_type.get('processing_error', 0) + 1
                        continue

                # 输出清洗统计
                logger.info(f"CSV文件处理完成，总行数: {total_rows}, 有效记录: {valid_count}")
                logger.info(f"数据清洗统计: 正常={self.cleaning_stats.valid_records}, "
                            f"警告={self.cleaning_stats.warning_records}, "
                            f"错误={self.cleaning_stats.error_records}")

        except Exception as e:
            logger.error(f"读取CSV文件时出错: {str(e)}")
            return []

        return decoded_records

    def _decode_jsonl_file(self, file_path: Path) -> List[ADSData]:
        """解码JSONL格式的ADS-B文件"""
        processed_records = []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
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
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.debug(f"第 {i} 行JSON解析错误: {str(e)}")
                        invalid_count += 1
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['json_decode'] = \
                            self.cleaning_stats.errors_by_type.get('json_decode', 0) + 1
                        continue

                    # 验证必要字段 - 针对示例JSONL格式调整
                    if 'latitude' not in data or 'longitude' not in data:
                        logger.debug(f"第 {i} 行缺少经纬度字段")
                        invalid_count += 1
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['missing_fields'] = \
                            self.cleaning_stats.errors_by_type.get('missing_fields', 0) + 1
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

                    # 检查飞机尾号
                    aircraft_tail = data.get('aircraft_tail', 'unknown')
                    if not aircraft_tail or aircraft_tail == 'unknown':
                        cleaning_notes.append("飞机尾号缺失")
                        data_status = "warning" if data_status == "normal" else data_status

                    # 解析坐标并验证
                    try:
                        lat = float(data.get('latitude', 0.0))
                        lon = float(data.get('longitude', 0.0))
                    except (ValueError, TypeError) as e:
                        logger.debug(f"第 {i} 行坐标格式错误: {str(e)}")
                        invalid_count += 1
                        self.cleaning_stats.error_records += 1
                        self.cleaning_stats.errors_by_type['coordinate_format'] = \
                            self.cleaning_stats.errors_by_type.get('coordinate_format', 0) + 1
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
                        data_status = "warning" if data_status == "normal" else data_status
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

                    if data_status == "normal":
                        self.cleaning_stats.valid_records += 1
                    elif data_status == "warning":
                        self.cleaning_stats.warning_records += 1
                    elif data_status == "error":
                        self.cleaning_stats.error_records += 1

                    # 每处理1000条记录输出一次进度
                    if i % 1000 == 0:
                        logger.info(f"已处理 {i}/{total_lines} 条ADS-B记录，有效: {valid_count}, 无效: {invalid_count}")

                except Exception as e:
                    logger.debug(f"处理第 {i} 行时出错: {str(e)}")
                    invalid_count += 1
                    self.cleaning_stats.error_records += 1
                    self.cleaning_stats.errors_by_type['processing_error'] = \
                        self.cleaning_stats.errors_by_type.get('processing_error', 0) + 1
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
            year = data.get('year', 2022)  # 默认2022年
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