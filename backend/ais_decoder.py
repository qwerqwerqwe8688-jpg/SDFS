import pyais
from datetime import datetime
import logging
from typing import List, Dict, Any
import re
import csv
from pathlib import Path
from .models import AISData, DataCleaningStats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AISDecoder:
    """AIS数据解码器 - 支持各种格式的AIS消息"""

    def __init__(self):
        self.decoded_data = []
        self.cleaning_stats = DataCleaningStats()

    def decode_ais_file(self, file_path: str) -> List[AISData]:
        """
        解码AIS文件中的所有数据，支持NMEA和CSV格式，包含数据清洗
        """
        logger.info(f"开始解码AIS文件: {file_path}")

        # 重置清洗统计
        self.cleaning_stats = DataCleaningStats()

        file_path = Path(file_path)
        if not file_path.exists():
            logger.error(f"AIS文件不存在: {file_path}")
            return []

        # 检测文件格式
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()

            if self._is_csv_format(first_line):
                logger.info("检测到CSV格式的AIS数据")
                return self._decode_csv_file(file_path)
            elif self._is_nmea_format(first_line):
                logger.info("检测到NMEA格式的AIS数据")
                return self._decode_nmea_file(file_path)
            else:
                logger.warning(f"无法识别的AIS文件格式，第一行: {first_line[:100]}...")
                # 尝试NMEA格式解码
                return self._decode_nmea_file(file_path)

        except Exception as e:
            logger.error(f"检测文件格式时出错: {str(e)}")
            return []

    def _is_csv_format(self, first_line: str) -> bool:
        """检查是否为CSV格式"""
        # CSV格式通常包含逗号分隔的列名
        csv_indicators = ['MMSI', 'BaseDateTime', 'LAT', 'LON', 'SOG', 'COG', 'Heading']
        return any(indicator in first_line for indicator in csv_indicators)

    def _is_nmea_format(self, first_line: str) -> bool:
        """检查是否为NMEA格式"""
        return first_line.startswith('!AIVDM') or first_line.startswith('!AIVDO')

    def _decode_csv_file(self, file_path: Path) -> List[AISData]:
        """解码CSV格式的AIS文件，包含数据清洗"""
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
                        mmsi = row.get('MMSI', '').strip()
                        lat_str = row.get('LAT', '').strip()
                        lon_str = row.get('LON', '').strip()

                        # 数据清洗检查
                        cleaning_notes = []
                        data_status = "normal"

                        # 检查核心字段缺失
                        if not mmsi or mmsi == 'unknown':
                            cleaning_notes.append("MMSI缺失")
                            data_status = "warning"
                            # 标记为待复核但不剔除

                        if not lat_str or not lon_str:
                            cleaning_notes.append("经纬度缺失")
                            data_status = "error"  # 没有位置数据，剔除
                            self.cleaning_stats.errors_by_type['missing_coordinates'] = \
                                self.cleaning_stats.errors_by_type.get('missing_coordinates', 0) + 1
                            continue

                        # 转换数据类型并验证
                        try:
                            lat = float(lat_str)
                            lon = float(lon_str)
                        except ValueError:
                            cleaning_notes.append(f"坐标格式错误: LAT={lat_str}, LON={lon_str}")
                            data_status = "error"
                            self.cleaning_stats.errors_by_type['coordinate_format'] = \
                                self.cleaning_stats.errors_by_type.get('coordinate_format', 0) + 1
                            continue

                        # 验证坐标范围
                        if not (-90 <= lat <= 90):
                            cleaning_notes.append(f"纬度超出范围: {lat}")
                            data_status = "error"
                            self.cleaning_stats.errors_by_type['latitude_range'] = \
                                self.cleaning_stats.errors_by_type.get('latitude_range', 0) + 1
                            continue

                        if not (-180 <= lon <= 180):
                            cleaning_notes.append(f"经度超出范围: {lon}")
                            data_status = "error"
                            self.cleaning_stats.errors_by_type['longitude_range'] = \
                                self.cleaning_stats.errors_by_type.get('longitude_range', 0) + 1
                            continue

                        # 检查异常坐标（如0,0海洋交叉点）
                        if lat == 0 and lon == 0:
                            cleaning_notes.append("可疑坐标(0,0)")
                            data_status = "warning"
                            self.cleaning_stats.warnings_by_type['suspicious_coordinates'] = \
                                self.cleaning_stats.warnings_by_type.get('suspicious_coordinates', 0) + 1

                        # 解析可选数值字段并进行清洗
                        sog = self._parse_float(row.get('SOG', '0'))
                        cog = self._parse_float(row.get('COG', '0'))
                        heading = self._parse_float(row.get('Heading', '0'))
                        length = self._parse_float(row.get('Length', '0'))
                        width = self._parse_float(row.get('Width', '0'))
                        draft = self._parse_float(row.get('Draft', '0'))

                        # 检查数值范围异常
                        if sog < 0 or sog > 50:  # 假设航速合理范围
                            cleaning_notes.append(f"航速异常: {sog}")
                            sog = max(0, min(sog, 50))  # 截断到合理范围
                            data_status = "warning" if data_status == "normal" else data_status
                            self.cleaning_stats.warnings_by_type['speed_range'] = \
                                self.cleaning_stats.warnings_by_type.get('speed_range', 0) + 1

                        if cog < 0 or cog > 360:
                            cleaning_notes.append(f"航向异常: {cog}")
                            cog = cog % 360  # 归一化到0-360度
                            data_status = "warning" if data_status == "normal" else data_status
                            self.cleaning_stats.warnings_by_type['course_range'] = \
                                self.cleaning_stats.warnings_by_type.get('course_range', 0) + 1

                        if heading < 0 or heading > 360:
                            cleaning_notes.append(f"船首向异常: {heading}")
                            heading = heading % 360  # 归一化到0-360度
                            data_status = "warning" if data_status == "normal" else data_status
                            self.cleaning_stats.warnings_by_type['heading_range'] = \
                                self.cleaning_stats.warnings_by_type.get('heading_range', 0) + 1

                        # 检查船舶尺寸合理性
                        if length > 0 and (length < 5 or length > 500):  # 假设船舶长度合理范围
                            cleaning_notes.append(f"船长异常: {length}")
                            data_status = "warning" if data_status == "normal" else data_status
                            self.cleaning_stats.warnings_by_type['length_range'] = \
                                self.cleaning_stats.warnings_by_type.get('length_range', 0) + 1

                        # 解析时间戳
                        timestamp_str = row.get('BaseDateTime', '')
                        timestamp = datetime.now()  # 默认值

                        if timestamp_str:
                            try:
                                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            except ValueError:
                                cleaning_notes.append(f"时间戳格式错误: {timestamp_str}")
                                data_status = "warning" if data_status == "normal" else data_status
                                self.cleaning_stats.warnings_by_type['timestamp_format'] = \
                                    self.cleaning_stats.warnings_by_type.get('timestamp_format', 0) + 1
                        else:
                            cleaning_notes.append("时间戳缺失")
                            data_status = "warning" if data_status == "normal" else data_status

                        # 解析船舶类型和状态
                        vessel_type_code = row.get('VesselType', '0')
                        try:
                            vessel_type_code_int = int(float(vessel_type_code))
                        except (ValueError, TypeError):
                            vessel_type_code_int = 0
                            cleaning_notes.append(f"船舶类型代码格式错误: {vessel_type_code}")
                            data_status = "warning" if data_status == "normal" else data_status

                        vessel_type = self._get_vessel_type(vessel_type_code_int)

                        status_code = row.get('Status', '')
                        nav_status = self._get_nav_status_from_code(status_code)

                        # 创建AIS数据对象
                        ais_data = AISData(
                            mmsi=mmsi if mmsi else "unknown",
                            latitude=lat,
                            longitude=lon,
                            sog=sog,
                            cog=cog,
                            heading=heading,
                            nav_status=nav_status,
                            vessel_type=vessel_type,
                            timestamp=timestamp,
                            data_type="ais",
                            data_status=data_status,
                            cleaning_notes="; ".join(cleaning_notes) if cleaning_notes else "",
                            vessel_name=row.get('VesselName', 'unknown').strip(),
                            imo=row.get('IMO', 'unknown').strip(),
                            call_sign=row.get('CallSign', 'unknown').strip(),
                            status=row.get('Status', 'unknown').strip(),
                            length=length,
                            width=width,
                            draft=draft,
                            cargo=row.get('Cargo', 'unknown').strip(),
                            transceiver_class=row.get('TransceiverClass', 'unknown').strip(),
                            base_date_time=timestamp_str
                        )

                        decoded_records.append(ais_data)
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

    def _decode_nmea_file(self, file_path: Path) -> List[AISData]:
        """解码NMEA格式的AIS文件，包含数据清洗"""
        decoded_records = []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            lines = content.strip().split('\n')
            total_lines = len(lines)
            logger.info(f"共读取到 {total_lines} 条AIS记录")

            # 用于存储多片段消息
            multi_part_messages = {}

            for i, line in enumerate(lines, 1):
                self.cleaning_stats.total_records += 1

                try:
                    line = line.strip()
                    if not line:
                        continue

                    # 验证是否为有效的AIS消息
                    if not (line.startswith('!AIVDM') or line.startswith('!AIVDO')):
                        logger.debug(f"第 {i} 行不是有效的AIS消息: {line[:50]}...")
                        continue

                    # 解析AIS消息头
                    parts = line.split(',')
                    if len(parts) < 7:
                        logger.debug(f"第 {i} 行格式不正确: {line[:50]}...")
                        continue

                    # 检查是否是多片段消息
                    try:
                        total_fragments = int(parts[1]) if parts[1] else 1
                        fragment_num = int(parts[2]) if parts[2] else 1
                        sequential_id = parts[3] if len(parts) > 3 else ''
                    except ValueError:
                        total_fragments = 1
                        fragment_num = 1
                        sequential_id = ''

                    message_key = f"{sequential_id}_{parts[5] if len(parts) > 5 else ''}"

                    if total_fragments > 1:
                        # 处理多片段消息
                        if message_key not in multi_part_messages:
                            multi_part_messages[message_key] = {}

                        multi_part_messages[message_key][fragment_num] = line

                        # 检查是否收集了所有片段
                        if len(multi_part_messages[message_key]) == total_fragments:
                            # 按顺序组装片段
                            fragments = []
                            for frag_num in sorted(multi_part_messages[message_key].keys()):
                                fragments.append(multi_part_messages[message_key][frag_num])

                            # 解码完整的多片段消息
                            for fragment in fragments:
                                try:
                                    ais_data = self._decode_single_nmea_message(fragment)
                                    if ais_data:
                                        decoded_records.append(ais_data)
                                except Exception as e:
                                    logger.debug(f"解码多片段消息子片段时出错: {str(e)}")

                            # 清理已处理的消息
                            del multi_part_messages[message_key]
                    else:
                        # 单片段消息，直接解码
                        ais_data = self._decode_single_nmea_message(line)
                        if ais_data:
                            decoded_records.append(ais_data)

                    # 每处理1000条记录输出一次进度
                    if i % 1000 == 0:
                        logger.info(f"已处理 {i}/{total_lines} 条记录，成功解码 {len(decoded_records)} 条")

                except Exception as e:
                    logger.warning(f"处理第 {i} 行时出错: {str(e)}")
                    self.cleaning_stats.error_records += 1
                    self.cleaning_stats.errors_by_type['processing_error'] = \
                        self.cleaning_stats.errors_by_type.get('processing_error', 0) + 1
                    continue

        except FileNotFoundError:
            logger.error(f"未找到AIS文件: {file_path}")
            return []
        except Exception as e:
            logger.error(f"读取AIS文件时出错: {str(e)}")
            return []

        # 处理剩余未完成的多片段消息（如果有）
        for message_key, fragments in multi_part_messages.items():
            if len(fragments) > 0:
                logger.warning(f"未完成的多片段消息 {message_key}: 收到 {len(fragments)} 个片段")

        logger.info(f"AIS解码完成，共获得 {len(decoded_records)} 条有效位置记录")
        logger.info(f"数据清洗统计: 正常={self.cleaning_stats.valid_records}, "
                    f"警告={self.cleaning_stats.warning_records}, "
                    f"错误={self.cleaning_stats.error_records}")

        return decoded_records

    def _decode_single_nmea_message(self, nmea_string: str) -> AISData:
        """解码单条NMEA格式的AIS消息，包含数据清洗"""
        try:
            # 使用pyais解码
            decoded = pyais.decode(nmea_string)

            # 将解码结果转换为字典
            if hasattr(decoded, 'asdict'):
                msg_dict = decoded.asdict()
            else:
                # 如果asdict不可用，尝试获取属性
                msg_dict = {}
                for attr in ['mmsi', 'lat', 'lon', 'sog', 'cog', 'heading', 'nav_status', 'type']:
                    if hasattr(decoded, attr):
                        msg_dict[attr] = getattr(decoded, attr)

            # 检查是否有位置信息
            if 'lat' in msg_dict and 'lon' in msg_dict:
                lat = msg_dict.get('lat')
                lon = msg_dict.get('lon')

                if lat is None or lon is None:
                    self.cleaning_stats.error_records += 1
                    self.cleaning_stats.errors_by_type['missing_coordinates'] = \
                        self.cleaning_stats.errors_by_type.get('missing_coordinates', 0) + 1
                    return None

                # 确保坐标值是数字
                try:
                    lat = float(lat)
                    lon = float(lon)
                except (ValueError, TypeError):
                    self.cleaning_stats.error_records += 1
                    self.cleaning_stats.errors_by_type['coordinate_format'] = \
                        self.cleaning_stats.errors_by_type.get('coordinate_format', 0) + 1
                    return None

                # 数据清洗检查
                cleaning_notes = []
                data_status = "normal"

                # 检查核心字段缺失
                mmsi = str(msg_dict.get('mmsi', 'unknown'))
                if not mmsi or mmsi == 'unknown':
                    cleaning_notes.append("MMSI缺失")
                    data_status = "warning"
                    self.cleaning_stats.warning_records += 1
                    self.cleaning_stats.warnings_by_type['missing_mmsi'] = \
                        self.cleaning_stats.warnings_by_type.get('missing_mmsi', 0) + 1
                else:
                    self.cleaning_stats.valid_records += 1

                # 跳过无效的坐标
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    self.cleaning_stats.error_records += 1
                    self.cleaning_stats.errors_by_type['coordinate_range'] = \
                        self.cleaning_stats.errors_by_type.get('coordinate_range', 0) + 1
                    return None

                # 检查异常坐标（如0,0海洋交叉点）
                if lat == 0 and lon == 0:
                    cleaning_notes.append("可疑坐标(0,0)")
                    data_status = "warning"
                    self.cleaning_stats.warnings_by_type['suspicious_coordinates'] = \
                        self.cleaning_stats.warnings_by_type.get('suspicious_coordinates', 0) + 1

                # 获取数值字段并进行清洗
                sog = float(msg_dict.get('sog', 0.0)) if msg_dict.get('sog') is not None else 0.0
                cog = float(msg_dict.get('cog', 0.0)) if msg_dict.get('cog') is not None else 0.0
                heading = float(msg_dict.get('heading', 0.0)) if msg_dict.get('heading') is not None else 0.0

                # 检查数值范围异常
                if sog < 0 or sog > 50:
                    cleaning_notes.append(f"航速异常: {sog}")
                    sog = max(0, min(sog, 50))
                    data_status = "warning" if data_status == "normal" else data_status

                if cog < 0 or cog > 360:
                    cleaning_notes.append(f"航向异常: {cog}")
                    cog = cog % 360
                    data_status = "warning" if data_status == "normal" else data_status

                if heading < 0 or heading > 360:
                    cleaning_notes.append(f"船首向异常: {heading}")
                    heading = heading % 360
                    data_status = "warning" if data_status == "normal" else data_status

                # 创建AIS数据对象
                ais_data = AISData(
                    mmsi=mmsi,
                    latitude=lat,
                    longitude=lon,
                    sog=sog,
                    cog=cog,
                    heading=heading,
                    nav_status=self._get_nav_status(msg_dict.get('nav_status')),
                    vessel_type=self._get_vessel_type(msg_dict.get('type')),
                    timestamp=datetime.now(),
                    data_status=data_status,
                    cleaning_notes="; ".join(cleaning_notes) if cleaning_notes else ""
                )
                return ais_data
            else:
                self.cleaning_stats.error_records += 1
                self.cleaning_stats.errors_by_type['no_position_data'] = \
                    self.cleaning_stats.errors_by_type.get('no_position_data', 0) + 1

        except Exception as e:
            logger.debug(f"解码消息时出错: {str(e)}")
            self.cleaning_stats.error_records += 1
            self.cleaning_stats.errors_by_type['decoding_error'] = \
                self.cleaning_stats.errors_by_type.get('decoding_error', 0) + 1
            return None

        return None

    def _parse_float(self, value: str) -> float:
        """安全地解析浮点数"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _get_nav_status_from_code(self, status_code: str) -> str:
        """根据CSV状态码获取航行状态"""
        if not status_code:
            return "unknown"

        try:
            code = int(float(status_code))
            return self._get_nav_status(code)
        except (ValueError, TypeError):
            return f"Unknown ({status_code})"

    def _get_nav_status(self, status_code) -> str:
        """获取航行状态"""
        if status_code is None:
            return "unknown"

        nav_status_map = {
            0: "Under way using engine",
            1: "At anchor",
            2: "Not under command",
            3: "Restricted maneuverability",
            4: "Constrained by her draught",
            5: "Moored",
            6: "Aground",
            7: "Engaged in fishing",
            8: "Under way sailing",
            9: "Reserved",
            10: "Reserved",
            11: "Reserved",
            12: "Reserved",
            13: "Reserved",
            14: "AIS-SART",
            15: "Undefined"
        }
        return nav_status_map.get(status_code, f"Unknown ({status_code})")

    def _get_vessel_type(self, vessel_type_code) -> str:
        """获取船舶类型"""
        if vessel_type_code is None:
            return "Unknown"

        vessel_type_map = {
            0: "Not available",
            1: "Reserved",
            2: "Reserved",
            3: "Reserved",
            4: "Reserved",
            5: "Reserved",
            6: "Reserved",
            7: "Reserved",
            8: "Reserved",
            9: "Reserved",
            10: "Reserved",
            11: "Reserved",
            12: "Reserved",
            13: "Reserved",
            14: "Reserved",
            15: "Reserved",
            16: "Reserved",
            17: "Reserved",
            18: "Reserved",
            19: "Reserved",
            20: "Wing in ground",
            21: "Wing in ground",
            22: "Wing in ground",
            23: "Wing in ground",
            24: "Wing in ground",
            25: "Wing in ground",
            26: "Wing in ground",
            27: "Wing in ground",
            28: "Wing in ground",
            29: "Wing in ground",
            30: "Fishing",
            31: "Towing",
            32: "Towing long",
            33: "Dredging",
            34: "Diving ops",
            35: "Military ops",
            36: "Sailing",
            37: "Pleasure craft",
            40: "High speed craft",
            41: "High speed craft",
            42: "High speed craft",
            43: "High speed craft",
            44: "High speed craft",
            45: "High speed craft",
            46: "High speed craft",
            47: "High speed craft",
            48: "High speed craft",
            49: "High speed craft",
            50: "Pilot vessel",
            51: "Search and rescue",
            52: "Tug",
            53: "Port tender",
            54: "Anti-pollution",
            55: "Law enforcement",
            56: "Spare",
            57: "Spare",
            58: "Medical transport",
            59: "Noncombatant",
            60: "Passenger",
            61: "Passenger",
            62: "Passenger",
            63: "Passenger",
            64: "Passenger",
            65: "Passenger",
            66: "Passenger",
            67: "Passenger",
            68: "Passenger",
            69: "Passenger",
            70: "Cargo",
            71: "Cargo",
            72: "Cargo",
            73: "Cargo",
            74: "Cargo",
            75: "Cargo",
            76: "Cargo",
            77: "Cargo",
            78: "Cargo",
            79: "Cargo",
            80: "Tanker",
            81: "Tanker",
            82: "Tanker",
            83: "Tanker",
            84: "Tanker",
            85: "Tanker",
            86: "Tanker",
            87: "Tanker",
            88: "Tanker",
            89: "Tanker",
            90: "Other",
            91: "Other",
            92: "Other",
            93: "Other",
            94: "Other",
            95: "Other",
            96: "Other",
            97: "Other",
            98: "Other",
            99: "Other"
        }
        return vessel_type_map.get(vessel_type_code, f"Unknown ({vessel_type_code})")

    def get_cleaning_stats(self) -> Dict[str, Any]:
        """获取数据清洗统计"""
        return self.cleaning_stats.to_dict()