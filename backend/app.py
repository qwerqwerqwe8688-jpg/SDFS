from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import logging
from datetime import datetime
import traceback
import os
import sys

from .config import Config
from .data_processor import DataProcessor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 创建Flask应用
app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)  # 允许跨域请求

# 配置
config = Config()
data_processor = DataProcessor()

# 全局变量存储处理后的数据
processed_data = None
last_update_time = None


def initialize_data():
    """初始化数据"""
    global processed_data, last_update_time

    try:
        logger.info("初始化数据...")

        # 检查缓存目录
        if not config.CACHE_DIR.exists():
            config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
            logger.info(f"创建缓存目录: {config.CACHE_DIR}")

        # 清理可能损坏的缓存
        temp_cache = config.PROCESSED_DATA_CACHE.with_suffix('.tmp')
        if temp_cache.exists():
            temp_cache.unlink(missing_ok=True)
            logger.info(f"清理临时缓存文件: {temp_cache}")

        # 处理数据
        processed_data = data_processor.process_all_data(force_update=False)
        last_update_time = datetime.now()

        if processed_data is None:
            logger.error("数据处理失败，返回None")
            return False

        logger.info(
            f"数据初始化成功: AIS={processed_data.get('metadata', {}).get('ais_count', 0)}, ADS-B={processed_data.get('metadata', {}).get('adsb_count', 0)}")
        return True

    except Exception as e:
        logger.error(f"初始化数据时出错: {str(e)}\n{traceback.format_exc()}")
        return False


@app.route('/')
def serve_frontend():
    """提供前端页面"""
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/<path:path>')
def serve_static_files(path):
    """提供静态文件"""
    return send_from_directory(app.static_folder, path)


@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查接口"""
    try:
        # 检查数据文件是否存在
        ais_nmea_exists = config.AIS_NMEA_FILE.exists()
        ais_csv_exists = config.AIS_CSV_FILE.exists()
        adsb_exists = config.ADSB_FILE.exists()

        # 检查数据状态
        data_ready = processed_data is not None

        return jsonify({
            'status': 'healthy',
            'service': 'SDFS Data Service',
            'timestamp': datetime.now().isoformat(),
            'data_files': {
                'ais_nmea': ais_nmea_exists,
                'ais_csv': ais_csv_exists,
                'adsb': adsb_exists
            },
            'data_status': {
                'processed': data_ready,
                'last_update': last_update_time.isoformat() if last_update_time else None,
                'ais_count': processed_data.get('metadata', {}).get('ais_count', 0) if data_ready else 0,
                'adsb_count': processed_data.get('metadata', {}).get('adsb_count', 0) if data_ready else 0,
                'ais_by_format': processed_data.get('metadata', {}).get('ais_by_format', {}) if data_ready else {}
            },
            'cache': {
                'exists': config.PROCESSED_DATA_CACHE.exists(),
                'size': config.PROCESSED_DATA_CACHE.stat().st_size if config.PROCESSED_DATA_CACHE.exists() else 0
            }
        })
    except Exception as e:
        logger.error(f"健康检查出错: {str(e)}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@app.route('/api/data/debug/ais', methods=['GET'])
def debug_ais():
    """调试AIS解码"""
    from backend.ais_decoder import AISDecoder

    try:
        decoder = AISDecoder()

        results = []

        # 测试所有AIS文件
        ais_files = config.get_ais_files()
        for ais_file in ais_files:
            logger.info(f"调试AIS文件: {ais_file.name}")

            # 读取前50行进行测试
            lines = []
            with open(ais_file, 'r', encoding='utf-8') as f:
                for i in range(50):
                    line = f.readline()
                    if not line:
                        break
                    lines.append(line.strip())

            for i, line in enumerate(lines, 1):
                try:
                    decoded = decoder._decode_single_nmea_message(line) if line.startswith('!AIVDM') or line.startswith(
                        '!AIVDO') else None
                    if decoded:
                        results.append({
                            'file': ais_file.name,
                            'line_number': i,
                            'line': line[:100] + '...' if len(line) > 100 else line,
                            'decoded': decoded.to_dict(),
                            'success': True
                        })
                    else:
                        # 尝试解析为CSV格式
                        if ',' in line and 'MMSI' not in line:  # 不是表头
                            parts = line.split(',')
                            if len(parts) >= 16:  # CSV格式有16列
                                results.append({
                                    'file': ais_file.name,
                                    'line_number': i,
                                    'line': line[:100] + '...' if len(line) > 100 else line,
                                    'success': True,
                                    'decoded': {
                                        'mmsi': parts[0],
                                        'latitude': float(parts[2]) if parts[2] else 0,
                                        'longitude': float(parts[3]) if parts[3] else 0,
                                        'sog': float(parts[4]) if parts[4] else 0,
                                        'cog': float(parts[5]) if parts[5] else 0,
                                        'data_type': 'csv'
                                    }
                                })
                            else:
                                results.append({
                                    'file': ais_file.name,
                                    'line_number': i,
                                    'line': line[:100] + '...' if len(line) > 100 else line,
                                    'success': False,
                                    'error': 'Invalid CSV format'
                                })
                        else:
                            results.append({
                                'file': ais_file.name,
                                'line_number': i,
                                'line': line[:100] + '...' if len(line) > 100 else line,
                                'success': False,
                                'error': 'No position data or invalid coordinates'
                            })
                except Exception as e:
                    results.append({
                        'file': ais_file.name,
                        'line_number': i,
                        'line': line[:100] + '...' if len(line) > 100 else line,
                        'success': False,
                        'error': str(e)
                    })

        successful_decodes = len([r for r in results if r['success']])
        return jsonify({
            'success': True,
            'results': results,
            'total_files': len(ais_files),
            'total_lines': len(results),
            'successful_decodes': successful_decodes,
            'failed_decodes': len(results) - successful_decodes,
            'success_rate': f"{(successful_decodes / len(results) * 100):.1f}%" if results else "0%"
        })

    except Exception as e:
        logger.error(f"AIS调试出错: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/data', methods=['GET'])
def get_data():
    """获取处理后的所有数据"""
    global processed_data, last_update_time

    try:
        force_update = request.args.get('force_update', 'false').lower() == 'true'
        refresh_cache = request.args.get('refresh_cache', 'false').lower() == 'true'

        if refresh_cache:
            # 强制清除缓存
            if config.PROCESSED_DATA_CACHE.exists():
                config.PROCESSED_DATA_CACHE.unlink()
                logger.info("缓存文件已清除")

        if processed_data is None or force_update:
            logger.info("开始处理数据..." if processed_data is None else "强制更新数据...")
            processed_data = data_processor.process_all_data(force_update=force_update)
            last_update_time = datetime.now()

        if processed_data is None:
            logger.error("数据处理失败，返回None")
            return jsonify({
                'success': False,
                'error': '数据处理失败',
                'message': '无法处理数据，请检查数据文件和日志'
            }), 500

        # 记录数据统计
        metadata = processed_data.get('metadata', {})
        logger.info(f"返回数据统计: AIS={metadata.get('ais_count', 0)}, ADS-B={metadata.get('adsb_count', 0)}")

        return jsonify({
            'success': True,
            'data': processed_data,
            'last_update': last_update_time.isoformat() if last_update_time else None,
            'metadata': metadata,
            'message': '数据获取成功'
        })

    except Exception as e:
        logger.error(f"获取数据时出错: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '数据获取失败'
        }), 500


@app.route('/api/data/stats', methods=['GET'])
def get_data_stats():
    """获取数据统计信息"""
    global processed_data

    try:
        if processed_data is None:
            return jsonify({
                'success': False,
                'message': '数据未初始化，请先获取数据'
            }), 400

        stats = {
            'total_records': processed_data.get('metadata', {}).get('total_records', 0),
            'ais_count': processed_data.get('metadata', {}).get('ais_count', 0),
            'ais_by_format': processed_data.get('metadata', {}).get('ais_by_format', {}),
            'adsb_count': processed_data.get('metadata', {}).get('adsb_count', 0),
            'coverage_layers_count': len(processed_data.get('coverage_layers', [])),
            'status_summary': processed_data.get('status_summary', {}),
            'last_update': last_update_time.isoformat() if last_update_time else None,
            'metadata': processed_data.get('metadata', {})
        }

        return jsonify({
            'success': True,
            'stats': stats,
            'message': '统计信息获取成功'
        })

    except Exception as e:
        logger.error(f"获取统计信息时出错: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '统计信息获取失败'
        }), 500


@app.route('/api/data/update', methods=['POST'])
def update_data():
    """更新数据（重新扫描文件）"""
    global processed_data, last_update_time

    try:
        logger.info("收到数据更新请求")

        # 清除缓存
        cache_files = [
            config.PROCESSED_DATA_CACHE,
            config.PROCESSED_DATA_CACHE.with_suffix('.tmp'),
            config.PROCESSED_DATA_CACHE.with_suffix('.bak')
        ]

        for cache_file in cache_files:
            if cache_file.exists():
                cache_file.unlink(missing_ok=True)
                logger.info(f"已清除缓存文件: {cache_file}")

        # 重新处理所有数据
        processed_data = data_processor.process_all_data(force_update=True)
        last_update_time = datetime.now()

        if processed_data is None:
            return jsonify({
                'success': False,
                'error': '数据处理失败',
                'message': '无法处理数据，请检查数据文件和日志'
            }), 500

        return jsonify({
            'success': True,
            'message': '数据更新成功',
            'last_update': last_update_time.isoformat(),
            'data_stats': {
                'total_records': processed_data.get('metadata', {}).get('total_records', 0),
                'ais_count': processed_data.get('metadata', {}).get('ais_count', 0),
                'adsb_count': processed_data.get('metadata', {}).get('adsb_count', 0),
                'ais_by_format': processed_data.get('metadata', {}).get('ais_by_format', {})
            }
        })

    except Exception as e:
        logger.error(f"更新数据时出错: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '数据更新失败'
        }), 500


@app.route('/api/data/coverage', methods=['GET'])
def get_coverage_layers():
    """获取覆盖范围图层数据"""
    global processed_data

    try:
        if processed_data is None:
            return jsonify({
                'success': False,
                'message': '数据未初始化，请先获取数据'
            }), 400

        return jsonify({
            'success': True,
            'coverage_layers': processed_data.get('coverage_layers', []),
            'message': '覆盖范围数据获取成功'
        })

    except Exception as e:
        logger.error(f"获取覆盖范围数据时出错: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '覆盖范围数据获取失败'
        }), 500


@app.route('/api/data/cache/clear', methods=['POST'])
def clear_cache():
    """清除缓存"""
    try:
        cache_files = [
            config.PROCESSED_DATA_CACHE,
            config.PROCESSED_DATA_CACHE.with_suffix('.tmp'),
            config.PROCESSED_DATA_CACHE.with_suffix('.bak')
        ]

        cleared_count = 0
        for cache_file in cache_files:
            if cache_file.exists():
                cache_file.unlink()
                cleared_count += 1
                logger.info(f"已清除缓存文件: {cache_file}")

        global processed_data, last_update_time
        processed_data = None
        last_update_time = None

        return jsonify({
            'success': True,
            'message': f'缓存已清除，共清理 {cleared_count} 个文件',
            'cleared_files': cleared_count
        })
    except Exception as e:
        logger.error(f"清除缓存时出错: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '清除缓存失败'
        }), 500


@app.route('/api/system/info', methods=['GET'])
def get_system_info():
    """获取系统信息"""
    try:
        # 计算文件行数
        def count_lines(file_path):
            try:
                if file_path.exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        return sum(1 for _ in f)
                return 0
            except:
                return 0

        # 计算文件大小
        def get_file_size(file_path):
            try:
                if file_path.exists():
                    return file_path.stat().st_size
                return 0
            except:
                return 0

        info = {
            'backend_version': '2.1',
            'python_version': sys.version,
            'data_directory': str(config.DATA_DIR),
            'cache_directory': str(config.CACHE_DIR),
            'data_files': {
                'ais_nmea': {
                    'exists': config.AIS_NMEA_FILE.exists(),
                    'size': get_file_size(config.AIS_NMEA_FILE),
                    'lines': count_lines(config.AIS_NMEA_FILE)
                },
                'ais_csv': {
                    'exists': config.AIS_CSV_FILE.exists(),
                    'size': get_file_size(config.AIS_CSV_FILE),
                    'lines': count_lines(config.AIS_CSV_FILE)
                },
                'adsb': {
                    'exists': config.ADSB_FILE.exists(),
                    'size': get_file_size(config.ADSB_FILE),
                    'lines': count_lines(config.ADSB_FILE)
                }
            },
            'cache': {
                'exists': config.PROCESSED_DATA_CACHE.exists(),
                'size': get_file_size(config.PROCESSED_DATA_CACHE),
                'temp_exists': config.PROCESSED_DATA_CACHE.with_suffix('.tmp').exists(),
                'bak_exists': config.PROCESSED_DATA_CACHE.with_suffix('.bak').exists()
            },
            'data_status': {
                'processed': processed_data is not None,
                'last_update': last_update_time.isoformat() if last_update_time else None,
                'ais_count': processed_data.get('metadata', {}).get('ais_count', 0) if processed_data else 0,
                'ais_by_format': processed_data.get('metadata', {}).get('ais_by_format', {}) if processed_data else {},
                'adsb_count': processed_data.get('metadata', {}).get('adsb_count', 0) if processed_data else 0
            }
        }

        return jsonify({
            'success': True,
            'system_info': info,
            'message': '系统信息获取成功'
        })
    except Exception as e:
        logger.error(f"获取系统信息时出错: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '系统信息获取失败'
        }), 500


@app.route('/api/debug/cache/content', methods=['GET'])
def get_cache_content():
    """获取缓存文件内容（用于调试）"""
    try:
        if not config.PROCESSED_DATA_CACHE.exists():
            return jsonify({
                'success': False,
                'error': '缓存文件不存在'
            }), 404

        with open(config.PROCESSED_DATA_CACHE, 'r', encoding='utf-8') as f:
            content = f.read()

        # 尝试解析JSON以验证格式
        try:
            data = json.loads(content)
            is_valid = True
            error = None
        except json.JSONDecodeError as e:
            data = None
            is_valid = False
            error = str(e)

        return jsonify({
            'success': True,
            'exists': True,
            'size': len(content),
            'is_valid_json': is_valid,
            'error': error,
            'preview': content[:1000] + '...' if len(content) > 1000 else content,
            'data_preview': {
                'metadata': data.get('metadata') if data else None,
                'ais_count': len(data.get('ais_data', [])) if data else 0,
                'ais_by_format': data.get('metadata', {}).get('ais_by_format', {}) if data else {},
                'adsb_count': len(data.get('adsb_data', [])) if data else 0
            } if data else None
        })

    except Exception as e:
        logger.error(f"获取缓存内容时出错: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("启动SDFS后端服务...")
    logger.info("=" * 60)

    logger.info(f"项目根目录: {config.BASE_DIR}")
    logger.info(f"数据目录: {config.DATA_DIR}")
    logger.info(f"缓存目录: {config.CACHE_DIR}")
    logger.info(f"前端目录: {app.static_folder}")

    # 检查数据文件
    ais_files = config.get_ais_files()
    if not ais_files:
        logger.warning(f"警告: 未找到任何AIS文件")
    else:
        for ais_file in ais_files:
            file_size = ais_file.stat().st_size
            logger.info(f"AIS文件: {ais_file.name} 大小: {file_size} 字节")

            # 显示文件信息
            try:
                with open(ais_file, 'r', encoding='utf-8') as f:
                    first_lines = [f.readline().strip() for _ in range(3) if f.readline()]
                    if first_lines:
                        logger.info(f"{ais_file.name} 前3行:")
                        for i, line in enumerate(first_lines):
                            logger.info(f"  行{i + 1}: {line[:80]}...")
            except Exception as e:
                logger.warning(f"读取AIS文件示例时出错: {e}")

    if not config.ADSB_FILE.exists():
        logger.warning(f"警告: ADS-B文件不存在: {config.ADSB_FILE}")
    else:
        file_size = config.ADSB_FILE.stat().st_size
        logger.info(f"ADS-B文件大小: {file_size} 字节")

    # 初始化数据
    if not initialize_data():
        logger.warning("数据初始化失败，将在首次请求时重试")

    logger.info("启动Flask应用...")
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)