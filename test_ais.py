#!/usr/bin/env python3
"""
AIS数据解码测试脚本
"""
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

from backend.ais_decoder import AISDecoder
from backend.config import Config


def test_ais_decoding():
    """测试AIS解码"""
    config = Config()

    if not config.AIS_FILE.exists():
        print(f"错误: AIS文件不存在: {config.AIS_FILE}")
        return

    print(f"测试AIS解码: {config.AIS_FILE}")
    print(f"文件大小: {config.AIS_FILE.stat().st_size} 字节")

    # 读取并显示前几行
    print("\n文件前10行:")
    with open(config.AIS_FILE, 'r') as f:
        for i in range(10):
            line = f.readline()
            if not line:
                break
            print(f"{i + 1:3d}: {line.strip()}")

    # 测试解码器
    decoder = AISDecoder()

    # 测试几个示例消息
    test_messages = [
        "!AIVDM,1,1,0,A,403sooQv25S`DW`0B4@qWO700@;f,0*77",
        "!AIVDM,1,1,0,B,H6:a2l@l4p`T4pLPtpL00000002,2*22",
        "!AIVDM,2,1,0,B,56:hGA0000000000000<P4pL`T4pLQD4pL`TsH0t51f8@5nV070j3kQlBCQh,0*68",
        "!AIVDM,2,2,0,B,0000000000>,2*29",
        "!AIVDM,1,1,0,B,16:VoL@P00WWs3p@tTO@0?w008CQ,0*40"
    ]

    print("\n测试示例消息解码:")
    for i, msg in enumerate(test_messages):
        print(f"\n测试消息 {i + 1}:")
        print(f"  消息: {msg}")
        try:
            result = decoder._decode_single_message(msg)
            if result:
                print(f"  解码成功:")
                print(f"    MMSI: {result.mmsi}")
                print(f"    位置: {result.latitude}, {result.longitude}")
                print(f"    航速: {result.sog}")
                print(f"    航向: {result.cog}")
                print(f"    类型: {result.vessel_type}")
            else:
                print(f"  解码失败: 无位置数据")
        except Exception as e:
            print(f"  解码失败: {e}")

    # 解码整个文件
    print("\n解码整个文件...")
    decoded_data = decoder.decode_ais_file(str(config.AIS_FILE))

    print(f"\n解码结果:")
    print(f"  总行数: 未知 (从日志查看)")
    print(f"  成功解码: {len(decoded_data)} 条")

    if decoded_data:
        print(f"\n前5条解码数据:")
        for i, data in enumerate(decoded_data[:5]):
            print(f"  {i + 1}. MMSI: {data.mmsi}, 位置: ({data.latitude:.6f}, {data.longitude:.6f}), 航速: {data.sog}节")

    return decoded_data


if __name__ == '__main__':
    test_ais_decoding()