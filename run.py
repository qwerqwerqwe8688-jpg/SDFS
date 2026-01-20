#!/usr/bin/env python3
"""
SDFS 系统启动脚本
"""
import os
import sys
import webbrowser
import threading
import time
from pathlib import Path
import subprocess


def check_dependencies():
    """检查依赖"""
    required_dirs = [
        's_data',
        'backend',
        'frontend',
        'data_cache'
    ]

    print("检查项目结构...")
    for dir_name in required_dirs:
        if not Path(dir_name).exists():
            print(f"  错误: 缺少目录 '{dir_name}'")
            return False
        print(f"  ✓ {dir_name}")

    # 检查数据文件
    data_files = ['s_data/AIS.txt', 's_data/ADSB.jsonl']
    for file_path in data_files:
        if not Path(file_path).exists():
            print(f"  警告: 数据文件 '{file_path}' 不存在")
        else:
            file_size = Path(file_path).stat().st_size
            print(f"  ✓ {file_path} ({file_size} 字节)")

    return True


def install_requirements():
    """安装Python依赖"""
    print("\n安装Python依赖...")
    if not Path('requirements.txt').exists():
        print("  错误: 找不到 requirements.txt")
        return False

    try:
        # 使用当前Python解释器安装依赖
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print("  ✓ 依赖安装完成")
            return True
        else:
            print(f"  错误: 安装依赖失败")
            print(f"  错误信息: {result.stderr}")
            return False
    except Exception as e:
        print(f"  错误: 安装依赖失败 - {e}")
        return False


def start_backend():
    """启动后端服务"""
    print("\n启动后端服务...")

    # 添加当前目录到Python路径
    sys.path.insert(0, str(Path(__file__).parent))

    try:
        from backend.app import app

        # 在单独的线程中启动Flask
        def run_flask():
            try:
                app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
            except Exception as e:
                print(f"  Flask运行错误: {e}")

        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()

        # 等待Flask启动
        print("  等待后端服务启动...")
        time.sleep(5)  # 增加等待时间

        # 检查服务是否启动
        import requests
        try:
            response = requests.get('http://localhost:5000/api/health', timeout=5)
            if response.status_code == 200:
                print("  ✓ 后端服务已启动 (http://localhost:5000)")
                return True
            else:
                print(f"  ✗ 后端服务启动失败，状态码: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            print("  ✗ 后端服务无法连接，可能启动失败")
            return False

    except ImportError as e:
        print(f"  错误: 无法导入后端模块 - {e}")
        print("  请确保已安装所有依赖: pip install -r requirements.txt")
        return False
    except Exception as e:
        print(f"  错误: 启动后端服务失败 - {e}")
        return False


def start_frontend():
    """启动前端"""
    print("\n启动前端...")

    frontend_path = Path('frontend/index.html').absolute()

    if not frontend_path.exists():
        print(f"  错误: 找不到前端文件 '{frontend_path}'")
        return False

    try:
        # 检查后端是否就绪
        import requests
        try:
            response = requests.get('http://localhost:5000/api/health', timeout=5)
            if response.status_code != 200:
                print("  警告: 后端服务可能未完全就绪")
        except:
            print("  警告: 无法连接到后端服务")

        # 打开浏览器
        url = f"file://{frontend_path}"
        print(f"  正在打开浏览器...")
        webbrowser.open(url)

        # 也提供本地服务器选项
        print(f"  或者，您可以手动在浏览器中打开: {frontend_path}")
        print(f"  或者使用本地HTTP服务器:")
        print(f"    1. cd {frontend_path.parent}")
        print(f"    2. python -m http.server 8080")
        print(f"    3. 打开 http://localhost:8080")

        return True
    except Exception as e:
        print(f"  错误: 启动前端失败 - {e}")
        return False


def cleanup_cache():
    """清理缓存"""
    print("\n清理缓存...")
    cache_dir = Path('data_cache')
    if cache_dir.exists():
        try:
            cache_files = list(cache_dir.glob('*'))
            for file in cache_files:
                file.unlink()
            print(f"  已清理 {len(cache_files)} 个缓存文件")
        except Exception as e:
            print(f"  清理缓存时出错: {e}")
    else:
        print("  缓存目录不存在，无需清理")


def main():
    """主函数"""
    print("=" * 60)
    print("SDFS - 数据资源地图可视化系统")
    print("=" * 60)

    # 清理旧缓存
    cleanup_cache()

    # 检查项目结构
    if not check_dependencies():
        print("\n项目结构不完整，请检查目录和文件。")
        sys.exit(1)

    # 安装依赖
    print("\n是否安装Python依赖? (y/n)")
    response = input("> ").strip().lower()
    if response == 'y':
        if not install_requirements():
            print("\n依赖安装失败，请手动安装。")
            response = input("是否继续? (y/n): ")
            if response.lower() != 'y':
                sys.exit(1)
    else:
        print("跳过依赖安装...")

    # 启动后端
    if not start_backend():
        print("\n后端启动失败。")
        response = input("是否继续尝试启动前端? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)

    # 启动前端
    if not start_frontend():
        print("\n前端启动失败。")

    print("\n" + "=" * 60)
    print("系统启动完成!")
    print("=" * 60)
    print("\n使用说明:")
    print("1. 首次启动请点击'加载数据'按钮")
    print("2. 点击'更新状态'按钮可以重新扫描数据文件")
    print("3. 使用控制面板过滤不同类型的数据")
    print("4. 点击地图上的标记查看详细信息")
    print("\n故障排除:")
    print("1. 如果数据加载失败，尝试点击'更新状态'按钮")
    print("2. 检查浏览器控制台是否有错误")
    print("3. 确保s_data目录中有正确的数据文件")
    print("\n按 Ctrl+C 停止系统")

    try:
        # 保持程序运行
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n正在停止SDFS系统...")
        sys.exit(0)


if __name__ == '__main__':
    main()