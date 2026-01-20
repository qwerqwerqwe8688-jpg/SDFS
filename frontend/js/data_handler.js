// 数据处理模块
class DataHandler {
    constructor() {
        this.backendUrl = 'http://localhost:5000';
        this.processedData = null;
        this.lastUpdate = null;
        this.isProcessing = false;
    }

    // 检查后端服务状态
    async checkBackendHealth() {
        try {
            const response = await axios.get(`${this.backendUrl}/api/health`, {
                timeout: 5000
            });
            return response.data.status === 'healthy';
        } catch (error) {
            console.error('后端服务不可用:', error);
            return false;
        }
    }

    // 加载数据
    async loadData(forceUpdate = false) {
        if (this.isProcessing) {
            return {
                success: false,
                message: '正在处理数据，请稍候...'
            };
        }

        this.isProcessing = true;
        updateSystemStatus('正在加载数据...', 'processing');

        try {
            const response = await axios.get(`${this.backendUrl}/api/data`, {
                params: { force_update: forceUpdate },
                timeout: 30000 // 30秒超时
            });

            if (response.data.success) {
                this.processedData = response.data.data;
                this.lastUpdate = response.data.last_update;

                updateSystemStatus('数据加载成功', 'success');
                return {
                    success: true,
                    data: this.processedData,
                    lastUpdate: this.lastUpdate,
                    message: '数据加载成功'
                };
            } else {
                throw new Error(response.data.message || '数据加载失败');
            }
        } catch (error) {
            console.error('加载数据时出错:', error);
            updateSystemStatus('数据加载失败', 'error');
            return {
                success: false,
                error: error.message,
                message: '数据加载失败，请检查后端服务'
            };
        } finally {
            this.isProcessing = false;
        }
    }

    // 更新数据（重新扫描）
    async updateData() {
        updateSystemStatus('正在更新数据...', 'processing');

        try {
            const response = await axios.post(`${this.backendUrl}/api/data/update`);

            if (response.data.success) {
                // 重新加载更新后的数据
                return await this.loadData(true);
            } else {
                throw new Error(response.data.message || '数据更新失败');
            }
        } catch (error) {
            console.error('更新数据时出错:', error);
            updateSystemStatus('数据更新失败', 'error');
            return {
                success: false,
                error: error.message,
                message: '数据更新失败'
            };
        }
    }

    // 获取数据统计
    async getDataStats() {
        try {
            const response = await axios.get(`${this.backendUrl}/api/data/stats`);

            if (response.data.success) {
                return {
                    success: true,
                    stats: response.data.stats,
                    message: '统计信息获取成功'
                };
            } else {
                throw new Error(response.data.message || '获取统计信息失败');
            }
        } catch (error) {
            console.error('获取统计信息时出错:', error);
            return {
                success: false,
                error: error.message,
                message: '获取统计信息失败'
            };
        }
    }

    // 获取覆盖范围数据
    async getCoverageLayers() {
        try {
            const response = await axios.get(`${this.backendUrl}/api/data/coverage`);

            if (response.data.success) {
                return {
                    success: true,
                    layers: response.data.coverage_layers,
                    message: '覆盖范围数据获取成功'
                };
            } else {
                throw new Error(response.data.message || '获取覆盖范围数据失败');
            }
        } catch (error) {
            console.error('获取覆盖范围数据时出错:', error);
            return {
                success: false,
                error: error.message,
                message: '获取覆盖范围数据失败'
            };
        }
    }

    // 获取AIS数据
    getAisData() {
        return this.processedData ? this.processedData.ais_data : [];
    }

    // 获取ADS-B数据
    getAdsbData() {
        return this.processedData ? this.processedData.adsb_data : [];
    }

    // 获取覆盖图层
    getCoverageLayersData() {
        return this.processedData ? this.processedData.coverage_layers : [];
    }

    // 获取状态统计
    getStatusSummary() {
        return this.processedData ? this.processedData.status_summary : null;
    }

    // 获取元数据
    getMetadata() {
        return this.processedData ? this.processedData.metadata : null;
    }

    // 判断数据点是否在线（基于时间戳）
    isDataPointOnline(timestamp) {
        if (!timestamp) return false;

        const dataTime = new Date(timestamp);
        const now = new Date();
        const hoursDiff = (now - dataTime) / (1000 * 60 * 60);

        return hoursDiff < 24; // 24小时内为在线
    }

    // 格式化数据点信息
    formatDataPointInfo(dataPoint) {
        const isOnline = this.isDataPointOnline(dataPoint.timestamp);

        let info = '';
        if (dataPoint.data_type === 'ais') {
            info = `
                <div class="data-item">
                    <h4>船舶信息 (AIS)</h4>
                    <p><strong>MMSI:</strong> ${dataPoint.mmsi}</p>
                    <p><strong>位置:</strong> ${dataPoint.latitude.toFixed(6)}, ${dataPoint.longitude.toFixed(6)}</p>
                    <p><strong>航速:</strong> ${dataPoint.sog.toFixed(1)} 节</p>
                    <p><strong>航向:</strong> ${dataPoint.cog.toFixed(1)}°</p>
                    <p><strong>船舶类型:</strong> ${dataPoint.vessel_type}</p>
                    <p><strong>航行状态:</strong> ${dataPoint.nav_status}</p>
                    <p><strong>时间:</strong> ${new Date(dataPoint.timestamp).toLocaleString()}</p>
                    <span class="status-badge ${isOnline ? 'online' : 'offline'}">
                        ${isOnline ? '在线' : '离线'}
                    </span>
                </div>
            `;
        } else if (dataPoint.data_type === 'adsb') {
            info = `
                <div class="data-item adsb">
                    <h4>飞机信息 (ADS-B)</h4>
                    <p><strong>飞机ID:</strong> ${dataPoint.aircraft_id}</p>
                    <p><strong>尾号:</strong> ${dataPoint.aircraft_tail}</p>
                    <p><strong>位置:</strong> ${dataPoint.latitude.toFixed(6)}, ${dataPoint.longitude.toFixed(6)}</p>
                    <p><strong>高度:</strong> ${dataPoint.altitude_ft.toFixed(0)} 英尺</p>
                    <p><strong>地速:</strong> ${dataPoint.ground_speed_kts.toFixed(0)} 节</p>
                    <p><strong>航向:</strong> ${dataPoint.heading_deg.toFixed(1)}°</p>
                    <p><strong>时间:</strong> ${new Date(dataPoint.timestamp).toLocaleString()}</p>
                    <span class="status-badge ${isOnline ? 'online' : 'offline'}">
                        ${isOnline ? '在线' : '离线'}
                    </span>
                </div>
            `;
        }

        return info;
    }

    // 格式化覆盖层信息
    formatCoverageLayerInfo(layer) {
        return `
            <div class="data-item">
                <h4>覆盖范围 (${layer.label})</h4>
                <p><strong>数据类型:</strong> ${layer.data_type.toUpperCase()}</p>
                <p><strong>状态:</strong> ${layer.status === 'online' ? '在线' : '离线'}</p>
                <p><strong>数据点数量:</strong> ${layer.metadata?.data_count || '未知'}</p>
                <p><strong>更新时间:</strong> ${new Date(layer.metadata?.update_time).toLocaleString()}</p>
                <p><strong>覆盖区域:</strong> ${layer.coordinates.length} 个坐标点</p>
            </div>
        `;
    }
}

// 创建全局数据处理器实例
const dataHandler = new DataHandler();

// 更新系统状态显示
function updateSystemStatus(message, type = 'info') {
    const statusElement = document.getElementById('system-status');
    const processingElement = document.getElementById('processing-status');

    statusElement.textContent = message;

    // 根据类型添加样式类
    statusElement.className = '';
    switch (type) {
        case 'success':
            statusElement.classList.add('status-success');
            break;
        case 'error':
            statusElement.classList.add('status-error');
            break;
        case 'processing':
            statusElement.classList.add('status-processing');
            break;
        default:
            statusElement.classList.add('status-info');
    }

    // 更新处理状态
    processingElement.textContent = `上次操作: ${new Date().toLocaleTimeString()}`;
}

// 更新界面统计信息
function updateStatsDisplay(stats) {
    if (!stats) return;

    // 更新数据计数
    document.getElementById('data-count').textContent = `数据总数: ${stats.total_records || 0}`;
    document.getElementById('ais-count').textContent = `(${stats.ais_count || 0})`;
    document.getElementById('adsb-count').textContent = `(${stats.adsb_count || 0})`;

    // 更新状态统计
    if (stats.status_summary) {
        document.getElementById('online-ais').textContent = stats.status_summary.online_ais || 0;
        document.getElementById('offline-ais').textContent = stats.status_summary.offline_ais || 0;
        document.getElementById('online-adsb').textContent = stats.status_summary.online_adsb || 0;
        document.getElementById('offline-adsb').textContent = stats.status_summary.offline_adsb || 0;
    }

    // 更新覆盖层计数
    document.getElementById('coverage-count').textContent = stats.coverage_layers_count || 0;

    // 更新最后更新时间
    if (stats.last_update) {
        document.getElementById('last-update').textContent =
            `最后更新: ${new Date(stats.last_update).toLocaleString()}`;
    }
}

// 更新后端状态显示
async function updateBackendStatus() {
    const isHealthy = await dataHandler.checkBackendHealth();
    const statusElement = document.getElementById('backend-status');

    if (isHealthy) {
        statusElement.textContent = '在线';
        statusElement.className = 'status-indicator status-online';
    } else {
        statusElement.textContent = '离线';
        statusElement.className = 'status-indicator status-offline';
    }

    return isHealthy;
}

// 初始化后端状态检查
updateBackendStatus();
setInterval(updateBackendStatus, 30000); // 每30秒检查一次