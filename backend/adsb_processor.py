import json
from datetime import datetime
from typing import List, Dict, Any
import logging
from .models import ADSData

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ADSBProcessor:
    """ADS-B数据处理器"""

    def __init__(self):
        self.processed_data = []

    def process_adsb_file(self, file_path: str) -> List[ADSData]:
        """
        处理ADS-B JSONL文件中的所有数据
        """
        logger.info(f"开始处理ADS-B文件: {file_path}")
        processed_records = []

        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()

            total_lines = len(lines)
            logger.info(f"共读取到 {total_lines} 条ADS-B记录")

            valid_count = 0
            invalid_count = 0

            for i, line in enumerate(lines, 1):
                try:
                    # 移除空白字符
                    line = line.strip()
                    if not line:
                        invalid_count += 1
                        continue

                    # 解析JSON行
                    data = json.loads(line)

                    # 验证必要字段
                    if not all(key in data for key in ['latitude', 'longitude', 'aircraft_id']):
                        invalid_count += 1
                        logger.debug(f"第 {i} 行缺少必要字段")
                        continue

                    # 创建ADS-B数据对象
                    adsb_data = ADSData(
                        aircraft_id=str(data.get('aircraft_id', 'unknown')),
                        latitude=float(data.get('latitude', 0.0)),
                        longitude=float(data.get('longitude', 0.0)),
                        altitude_ft=float(data.get('altitude_ft', 0.0)),
                        ground_speed_kts=float(data.get('ground_speed_kts', 0.0)),
                        heading_deg=float(data.get('heading_deg', 0.0)),
                        aircraft_tail=data.get('aircraft_tail', 'unknown'),
                        timestamp=self._parse_timestamp(data)
                    )
                    processed_records.append(adsb_data)
                    valid_count += 1

                    # 每处理1000条记录输出一次进度
                    if i % 1000 == 0:
                        logger.info(f"已处理 {i}/{total_lines} 条ADS-B记录，有效: {valid_count}, 无效: {invalid_count}")

                except json.JSONDecodeError as e:
                    invalid_count += 1
                    logger.debug(f"解析第 {i} 行JSON时出错: {str(e)}")
                    continue
                except Exception as e:
                    invalid_count += 1
                    logger.debug(f"处理第 {i} 行时出错: {str(e)}")
                    continue

        except FileNotFoundError:
            logger.error(f"未找到ADS-B文件: {file_path}")
            return []
        except Exception as e:
            logger.error(f"读取ADS-B文件时出错: {str(e)}")
            return []

        logger.info(f"ADS-B处理完成，有效记录: {valid_count}, 无效记录: {invalid_count}")
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