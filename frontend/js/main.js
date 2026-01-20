// 主应用程序模块
document.addEventListener('DOMContentLoaded', () => {
    // 初始化地图
    mapVisualization.initializeMap();

    // 检查后端状态并加载数据
    checkBackendAndLoadData();

    // 绑定事件监听器
    bindEventListeners();

    // 设置过滤器的变更监听
    setupFilterListeners();

    // 添加调试快捷键
    addDebugShortcuts();

    console.log('SDFS前端系统初始化完成');
});

// 检查后端状态并加载数据
async function checkBackendAndLoadData() {
    updateSystemStatus('正在检查后端服务...', 'processing');

    // 检查后端健康状态
    const isBackendHealthy = await dataHandler.checkBackendHealth();

    if (!isBackendHealthy) {
        updateSystemStatus('后端服务不可用', 'error');
        showErrorMessage('后端服务不可用',
            '请确保后端服务已启动并运行在 http://localhost:5000。<br>' +
            '您可以：<br>' +
            '1. 检查后端是否正在运行<br>' +
            '2. 检查防火墙或端口设置<br>' +
            '3. 重新启动系统'
        );
        return;
    }

    // 获取系统信息
    try {
        const response = await axios.get('http://localhost:5000/api/system/info');
        if (response.data.success) {
            const info = response.data.system_info;
            console.log('系统信息:', info);

            // 显示数据文件状态
            if (!info.data_files.ais.exists || !info.data_files.adsb.exists) {
                showWarningMessage('数据文件缺失',
                    `AIS文件: ${info.data_files.ais.exists ? '存在' : '缺失'}<br>
                     ADS-B文件: ${info.data_files.adsb.exists ? '存在' : '缺失'}<br>
                     请确保s_data目录中有正确的数据文件。`);
            }

            // 如果AIS文件存在但很小，可能有问题
            if (info.data_files.ais.exists && info.data_files.ais.size < 100) {
                showWarningMessage('AIS文件可能有问题',
                    `AIS文件大小仅为 ${info.data_files.ais.size} 字节，可能包含的数据太少。`);
            }

            // 显示数据文件行数
            console.log(`AIS文件行数: ${info.data_files.ais.lines}`);
            console.log(`ADS-B文件行数: ${info.data_files.adsb.lines}`);
        }
    } catch (error) {
        console.warn('获取系统信息失败:', error);
    }

    updateSystemStatus('正在加载数据...', 'processing');

    // 加载数据，强制刷新缓存
    const result = await dataHandler.loadData(true);

    if (result.success) {
        // 更新地图数据
        const aisData = dataHandler.getAisData();
        const adsbData = dataHandler.getAdsbData();
        const coverageLayers = dataHandler.getCoverageLayersData();

        mapVisualization.updateMapData(aisData, adsbData, coverageLayers);

        // 更新统计信息
        const statsResult = await dataHandler.getDataStats();
        if (statsResult.success) {
            updateStatsDisplay(statsResult.stats);
        }

        updateSystemStatus('数据加载完成', 'success');

        // 显示加载结果消息
        const totalRecords = aisData.length + adsbData.length;
        if (totalRecords > 0) {
            showSuccessMessage('数据加载成功',
                `共加载 ${totalRecords} 条数据<br>
                 AIS: ${aisData.length} 条<br>
                 ADS-B: ${adsbData.length} 条<br>
                 覆盖范围: ${coverageLayers.length} 个图层`);
        } else {
            showWarningMessage('数据加载完成',
                '成功加载但未找到任何有效数据。<br>' +
                '这可能是因为：<br>' +
                '1. 数据文件格式不正确<br>' +
                '2. 数据文件中没有有效的位置信息<br>' +
                '3. 数据解析过程中出现错误');
        }

        // 如果AIS数据为0而ADS-B数据正常，建议调试
        if (aisData.length === 0 && adsbData.length > 0) {
            showWarningMessage('AIS数据解码可能有问题',
                `ADS-B数据加载成功 (${adsbData.length} 条)，但AIS数据为0条。<br>
                 这可能是因为AIS数据格式问题。点击<a href="#" onclick="testAISDecoding()" style="color: #3498db; text-decoration: underline;">这里</a>进行AIS解码测试。`);
        }
    } else {
        updateSystemStatus('数据加载失败', 'error');
        showErrorMessage('数据加载失败',
            result.message || '未知错误<br><br>' +
            '您可以尝试：<br>' +
            '1. 点击"更新状态"按钮重新处理数据<br>' +
            '2. 检查数据文件格式是否正确<br>' +
            '3. 查看后端日志获取详细错误信息'
        );

        // 尝试清除缓存并重试
        console.log('尝试清除缓存并重试...');
        try {
            await axios.post('http://localhost:5000/api/data/cache/clear');
            const retryResult = await dataHandler.loadData(true);
            if (retryResult.success) {
                showSuccessMessage('重试成功', '清除缓存后数据加载成功，页面将重新加载...');
                setTimeout(() => {
                    location.reload();
                }, 2000);
            }
        } catch (retryError) {
            console.error('重试失败:', retryError);
        }
    }
}

// 绑定事件监听器
function bindEventListeners() {
    // 加载数据按钮
    document.getElementById('load-data-btn').addEventListener('click', async () => {
        await loadInitialData();
    });

    // 更新数据按钮
    document.getElementById('update-data-btn').addEventListener('click', async () => {
        if (!confirm('确定要重新扫描数据文件吗？这可能需要一些时间。')) {
            return;
        }

        updateSystemStatus('正在更新数据...', 'processing');

        const result = await dataHandler.updateData();

        if (result.success) {
            // 更新地图数据
            const aisData = dataHandler.getAisData();
            const adsbData = dataHandler.getAdsbData();
            const coverageLayers = dataHandler.getCoverageLayersData();

            mapVisualization.updateMapData(aisData, adsbData, coverageLayers);

            // 更新统计信息
            const statsResult = await dataHandler.getDataStats();
            if (statsResult.success) {
                updateStatsDisplay(statsResult.stats);
            }

            updateSystemStatus('数据更新完成', 'success');
            showSuccessMessage('数据更新成功',
                `数据更新完成！<br>
                 共加载 ${aisData.length + adsbData.length} 条数据<br>
                 AIS: ${aisData.length} 条<br>
                 ADS-B: ${adsbData.length} 条`);
        } else {
            updateSystemStatus('数据更新失败', 'error');
            showErrorMessage('数据更新失败', result.message || '未知错误');
        }
    });

    // 重置视图按钮
    document.getElementById('reset-view-btn').addEventListener('click', () => {
        mapVisualization.resetView();
        showSuccessMessage('视图已重置', '地图视图已重置到初始位置和缩放级别。');
    });

    // 模态框关闭按钮
    document.querySelector('.close-modal').addEventListener('click', () => {
        document.getElementById('info-modal').style.display = 'none';
    });

    // 点击模态框外部关闭
    window.addEventListener('click', (event) => {
        const modal = document.getElementById('info-modal');
        if (event.target === modal) {
            modal.style.display = 'none';
        }
    });

    // 键盘快捷键
    document.addEventListener('keydown', (event) => {
        // ESC键关闭模态框
        if (event.key === 'Escape') {
            document.getElementById('info-modal').style.display = 'none';
        }

        // Ctrl+R 重新加载数据
        if (event.ctrlKey && event.key === 'r') {
            event.preventDefault();
            document.getElementById('load-data-btn').click();
        }

        // Ctrl+U 更新数据
        if (event.ctrlKey && event.key === 'u') {
            event.preventDefault();
            document.getElementById('update-data-btn').click();
        }

        // Ctrl+H 重置视图
        if (event.ctrlKey && event.key === 'h') {
            event.preventDefault();
            document.getElementById('reset-view-btn').click();
        }
    });
}

// 设置过滤器监听
function setupFilterListeners() {
    const filters = ['filter-ais', 'filter-adsb', 'filter-coverage'];

    filters.forEach(filterId => {
        document.getElementById(filterId).addEventListener('change', () => {
            mapVisualization.updateVisibility();
            updateFilterStatus();
        });
    });
}

// 更新过滤状态显示
function updateFilterStatus() {
    const aisChecked = document.getElementById('filter-ais').checked;
    const adsbChecked = document.getElementById('filter-adsb').checked;
    const coverageChecked = document.getElementById('filter-coverage').checked;

    let status = '当前显示: ';
    const filters = [];

    if (aisChecked) filters.push('AIS');
    if (adsbChecked) filters.push('ADS-B');
    if (coverageChecked) filters.push('覆盖范围');

    if (filters.length === 0) {
        status += '无';
    } else {
        status += filters.join(', ');
    }

    updateSystemStatus(status, 'info');
}

// 添加调试快捷键
function addDebugShortcuts() {
    document.addEventListener('keydown', (event) => {
        // Ctrl+Shift+D 打开调试面板
        if (event.ctrlKey && event.shiftKey && event.key === 'D') {
            event.preventDefault();
            toggleDebugPanel();
        }

        // Ctrl+Shift+L 重新加载数据
        if (event.ctrlKey && event.shiftKey && event.key === 'L') {
            event.preventDefault();
            loadInitialData();
        }

        // Ctrl+Shift+C 清除缓存
        if (event.ctrlKey && event.shiftKey && event.key === 'C') {
            event.preventDefault();
            clearCache();
        }

        // Ctrl+Shift+T 测试AIS解码
        if (event.ctrlKey && event.shiftKey && event.key === 'T') {
            event.preventDefault();
            testAISDecoding();
        }
    });
}

// 切换调试面板
function toggleDebugPanel() {
    let debugPanel = document.getElementById('debug-panel');
    if (!debugPanel) {
        createDebugPanel();
    } else {
        debugPanel.style.display = debugPanel.style.display === 'none' ? 'block' : 'none';
    }
}

// 创建调试面板
function createDebugPanel() {
    const debugPanel = document.createElement('div');
    debugPanel.id = 'debug-panel';
    debugPanel.style.cssText = `
        position: fixed;
        top: 60px;
        right: 20px;
        width: 450px;
        max-height: 600px;
        background: white;
        border: 2px solid #3498db;
        border-radius: 5px;
        padding: 15px;
        z-index: 10000;
        box-shadow: 0 4px 20px rgba(0,0,0,0.2);
        overflow-y: auto;
        font-family: monospace;
        font-size: 12px;
    `;

    debugPanel.innerHTML = `
        <h3 style="margin-top: 0; color: #3498db; border-bottom: 2px solid #3498db; padding-bottom: 5px;">调试面板</h3>
        <div style="margin-bottom: 10px;">
            <button id="refresh-debug" style="margin: 2px; padding: 5px 10px; background: #3498db; color: white; border: none; border-radius: 3px; cursor: pointer;">刷新信息</button>
            <button id="clear-cache-debug" style="margin: 2px; padding: 5px 10px; background: #e74c3c; color: white; border: none; border-radius: 3px; cursor: pointer;">清除缓存</button>
            <button id="reload-data-debug" style="margin: 2px; padding: 5px 10px; background: #2ecc71; color: white; border: none; border-radius: 3px; cursor: pointer;">重新加载数据</button>
            <button id="test-ais-debug" style="margin: 2px; padding: 5px 10px; background: #f39c12; color: white; border: none; border-radius: 3px; cursor: pointer;">测试AIS解码</button>
        </div>
        <hr style="margin: 10px 0;">
        <div id="debug-info" style="margin-top: 10px;"></div>
        <div style="margin-top: 10px; font-size: 10px; color: #7f8c8d; text-align: center;">
            调试面板 - 按 Ctrl+Shift+D 关闭
        </div>
    `;

    document.body.appendChild(debugPanel);

    // 绑定调试按钮事件
    document.getElementById('refresh-debug').addEventListener('click', updateDebugInfo);
    document.getElementById('clear-cache-debug').addEventListener('click', clearCache);
    document.getElementById('reload-data-debug').addEventListener('click', () => {
        loadInitialData();
        updateDebugInfo();
    });
    document.getElementById('test-ais-debug').addEventListener('click', testAISDecoding);

    updateDebugInfo();
}

// 更新调试信息
async function updateDebugInfo() {
    const debugInfo = document.getElementById('debug-info');
    if (!debugInfo) return;

    debugInfo.innerHTML = '<p>正在获取调试信息...</p>';

    try {
        const [healthRes, systemRes, statsRes] = await Promise.allSettled([
            axios.get('http://localhost:5000/api/health'),
            axios.get('http://localhost:5000/api/system/info'),
            axios.get('http://localhost:5000/api/data/stats')
        ]);

        let infoHtml = '';

        // 后端状态
        if (healthRes.status === 'fulfilled') {
            infoHtml += `<h4>后端状态</h4><pre>${JSON.stringify(healthRes.value.data, null, 2)}</pre>`;
        } else {
            infoHtml += `<h4 style="color: #e74c3c;">后端状态获取失败</h4><pre>${healthRes.reason.message}</pre>`;
        }

        // 系统信息
        if (systemRes.status === 'fulfilled') {
            infoHtml += `<h4>系统信息</h4><pre>${JSON.stringify(systemRes.value.data.system_info, null, 2)}</pre>`;
        } else {
            infoHtml += `<h4 style="color: #e74c3c;">系统信息获取失败</h4><pre>${systemRes.reason.message}</pre>`;
        }

        // 数据统计
        if (statsRes.status === 'fulfilled') {
            infoHtml += `<h4>数据统计</h4><pre>${JSON.stringify(statsRes.value.data.stats, null, 2)}</pre>`;
        } else {
            infoHtml += `<h4 style="color: #e74c3c;">数据统计获取失败</h4><pre>${statsRes.reason.message}</pre>`;
        }

        // 前端状态
        const frontendStatus = {
            timestamp: new Date().toISOString(),
            aisMarkers: mapVisualization.aisMarkers.size,
            adsbMarkers: mapVisualization.adsbMarkers.size,
            coveragePolygons: mapVisualization.coveragePolygons.size,
            mapZoom: mapVisualization.map?.getZoom() || 'N/A',
            mapCenter: mapVisualization.map?.getCenter() || 'N/A',
            filters: {
                ais: document.getElementById('filter-ais').checked,
                adsb: document.getElementById('filter-adsb').checked,
                coverage: document.getElementById('filter-coverage').checked
            },
            windowSize: {
                width: window.innerWidth,
                height: window.innerHeight
            }
        };

        infoHtml += `<h4>前端状态</h4><pre>${JSON.stringify(frontendStatus, null, 2)}</pre>`;

        debugInfo.innerHTML = infoHtml;
    } catch (error) {
        debugInfo.innerHTML = `<p style="color: red;">获取调试信息失败: ${error.message}</p>`;
    }
}

// 清除缓存
async function clearCache() {
    try {
        updateSystemStatus('正在清除缓存...', 'processing');

        const response = await axios.post('http://localhost:5000/api/data/cache/clear');

        if (response.data.success) {
            updateSystemStatus('缓存已清除', 'success');
            showSuccessMessage('缓存已清除', '缓存文件已成功清除。');

            // 更新调试面板
            updateDebugInfo();
        } else {
            showErrorMessage('清除缓存失败', response.data.message || '未知错误');
        }
    } catch (error) {
        showErrorMessage('清除缓存失败', error.message);
    }
}

// AIS解码测试
async function testAISDecoding() {
    try {
        updateSystemStatus('正在测试AIS解码...', 'processing');

        const response = await axios.get('http://localhost:5000/api/data/debug/ais');

        if (response.data.success) {
            const results = response.data.results;
            const successful = response.data.successful_decodes;
            const total = response.data.total_lines;

            let message = `测试了 ${total} 行AIS数据，成功解码 ${successful} 行。<br><br>`;
            message += `<button onclick="showDetailedAISResults()" style="margin: 10px; padding: 8px 16px; background: #3498db; color: white; border: none; border-radius: 3px; cursor: pointer;">查看详细结果</button>`;

            if (successful === 0) {
                message += `<br><br><strong>问题分析:</strong><br>`;
                message += `1. 检查AIS文件格式是否正确<br>`;
                message += `2. 确保pyais库已正确安装 (pip install pyais)<br>`;
                message += `3. 检查AIS消息是否符合标准格式<br>`;
                message += `4. 可能是多片段消息处理问题`;
            }

            // 存储结果供详细查看
            window.aisDebugResults = results;

            showSuccessMessage('AIS解码测试完成', message);
            updateSystemStatus('AIS解码测试完成', 'success');
        } else {
            showErrorMessage('AIS解码测试失败', response.data.error || '未知错误');
        }
    } catch (error) {
        showErrorMessage('AIS解码测试出错', error.message);
    }
}

// 显示详细的AIS解码结果
function showDetailedAISResults() {
    if (!window.aisDebugResults) {
        showErrorMessage('没有测试结果', '请先运行AIS解码测试');
        return;
    }

    const modal = document.getElementById('info-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');

    modalTitle.textContent = 'AIS解码详细结果';

    let html = `
        <div style="margin-bottom: 15px;">
            <button onclick="exportAISResults()" style="margin: 5px; padding: 8px 16px; background: #2ecc71; color: white; border: none; border-radius: 3px; cursor: pointer;">导出结果为JSON</button>
            <button onclick="runFullAISDecode()" style="margin: 5px; padding: 8px 16px; background: #3498db; color: white; border: none; border-radius: 3px; cursor: pointer;">运行完整解码</button>
            <button onclick="copyAISResults()" style="margin: 5px; padding: 8px 16px; background: #9b59b6; color: white; border: none; border-radius: 3px; cursor: pointer;">复制到剪贴板</button>
        </div>
        <div style="max-height: 400px; overflow-y: auto;">
            <table style="width: 100%; border-collapse: collapse; font-size: 12px;">
                <thead>
                    <tr style="background-color: #f2f2f2; position: sticky; top: 0;">
                        <th style="padding: 8px; border: 1px solid #ddd; text-align: left; width: 60px;">行号</th>
                        <th style="padding: 8px; border: 1px solid #ddd; text-align: left; width: 80px;">状态</th>
                        <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">AIS消息</th>
                        <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">解码信息</th>
                    </tr>
                </thead>
                <tbody>
    `;

    window.aisDebugResults.forEach(result => {
        const statusColor = result.success ? '#27ae60' : '#e74c3c';
        const statusText = result.success ? '✓ 成功' : '✗ 失败';

        html += `
            <tr>
                <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">${result.line_number}</td>
                <td style="padding: 8px; border: 1px solid #ddd; color: ${statusColor}; font-weight: bold;">${statusText}</td>
                <td style="padding: 8px; border: 1px solid #ddd; font-family: monospace; font-size: 11px; word-break: break-all;">${result.line}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">
        `;

        if (result.success) {
            html += `
                <strong>MMSI:</strong> ${result.decoded.mmsi}<br>
                <strong>位置:</strong> ${result.decoded.latitude.toFixed(6)}, ${result.decoded.longitude.toFixed(6)}<br>
                <strong>航速:</strong> ${result.decoded.sog} 节<br>
                <strong>航向:</strong> ${result.decoded.cog}°<br>
                <strong>类型:</strong> ${result.decoded.vessel_type}<br>
                <strong>状态:</strong> ${result.decoded.nav_status}
            `;
        } else {
            html += `<span style="color: #e74c3c;">${result.error}</span>`;
        }

        html += `</td></tr>`;
    });

    html += `
                </tbody>
            </table>
        </div>
        <div style="margin-top: 15px; padding: 10px; background-color: #f8f9fa; border-radius: 5px; font-size: 12px;">
            <strong>总结:</strong> 共测试 ${window.aisDebugResults.length} 行，成功 ${window.aisDebugResults.filter(r => r.success).length} 行，失败 ${window.aisDebugResults.filter(r => !r.success).length} 行。
        </div>
    `;

    modalBody.innerHTML = html;
    modal.style.display = 'block';
}

// 导出AIS结果
function exportAISResults() {
    if (!window.aisDebugResults) {
        showErrorMessage('没有结果可导出', '请先运行AIS解码测试');
        return;
    }

    try {
        const dataStr = JSON.stringify(window.aisDebugResults, null, 2);
        const dataBlob = new Blob([dataStr], { type: 'application/json' });
        const dataUrl = URL.createObjectURL(dataBlob);

        const link = document.createElement('a');
        link.href = dataUrl;
        link.download = `ais_debug_${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        URL.revokeObjectURL(dataUrl);

        showSuccessMessage('导出成功', 'AIS解码结果已导出为JSON文件。');
    } catch (error) {
        showErrorMessage('导出失败', error.message);
    }
}

// 复制AIS结果到剪贴板
async function copyAISResults() {
    if (!window.aisDebugResults) {
        showErrorMessage('没有结果可复制', '请先运行AIS解码测试');
        return;
    }

    try {
        const summary = `AIS解码测试结果\n` +
                       `测试时间: ${new Date().toISOString()}\n` +
                       `总行数: ${window.aisDebugResults.length}\n` +
                       `成功: ${window.aisDebugResults.filter(r => r.success).length}\n` +
                       `失败: ${window.aisDebugResults.filter(r => !r.success).length}\n\n`;

        const details = window.aisDebugResults.map(r =>
            `行 ${r.line_number}: ${r.success ? '成功' : '失败'} - ${r.line}\n` +
            `${r.success ?
                `  MMSI: ${r.decoded.mmsi}, 位置: (${r.decoded.latitude}, ${r.decoded.longitude})` :
                `  错误: ${r.error}`}\n`
        ).join('\n');

        const textToCopy = summary + details;

        await navigator.clipboard.writeText(textToCopy);
        showSuccessMessage('复制成功', 'AIS解码结果已复制到剪贴板。');
    } catch (error) {
        showErrorMessage('复制失败', error.message);
    }
}

// 运行完整AIS解码
async function runFullAISDecode() {
    try {
        updateSystemStatus('正在运行完整AIS解码...', 'processing');

        // 先清除缓存
        await axios.post('http://localhost:5000/api/data/cache/clear');

        // 重新加载数据
        const result = await dataHandler.loadData(true);

        if (result.success) {
            const aisData = dataHandler.getAisData();
            const adsbData = dataHandler.getAdsbData();

            let message = `完整解码完成！<br><br>`;
            message += `AIS数据: ${aisData.length} 条<br>`;
            message += `ADS-B数据: ${adsbData.length} 条<br><br>`;

            if (aisData.length === 0) {
                message += `<strong>注意:</strong> AIS数据仍然为0条，可能存在以下问题：<br>`;
                message += `1. AIS文件格式不正确<br>`;
                message += `2. AIS消息不包含位置信息<br>`;
                message += `3. pyais库解码失败<br><br>`;
                message += `建议检查AIS文件内容。`;
            } else {
                message += `页面将在2秒后重新加载以显示新数据...`;
                setTimeout(() => {
                    location.reload();
                }, 2000);
            }

            showSuccessMessage('完整解码完成', message);
            updateSystemStatus('完整解码完成', 'success');
        } else {
            showErrorMessage('完整解码失败', result.message || '未知错误');
        }
    } catch (error) {
        showErrorMessage('完整解码出错', error.message);
    }
}

// 更新系统状态显示
function updateSystemStatus(message, type = 'info') {
    const statusElement = document.getElementById('system-status');
    const processingElement = document.getElementById('processing-status');

    statusElement.textContent = message;

    // 清除所有状态类
    statusElement.className = '';

    // 根据类型添加样式类
    switch (type) {
        case 'success':
            statusElement.classList.add('status-success');
            break;
        case 'error':
            statusElement.classList.add('status-error');
            break;
        case 'warning':
            statusElement.classList.add('status-warning');
            break;
        case 'processing':
            statusElement.classList.add('status-processing');
            break;
        default:
            statusElement.classList.add('status-info');
    }

    // 更新处理状态时间
    if (type !== 'processing') {
        processingElement.textContent = `上次操作: ${new Date().toLocaleTimeString()}`;
    }
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
        const lastUpdate = new Date(stats.last_update);
        const now = new Date();
        const diffMinutes = Math.floor((now - lastUpdate) / (1000 * 60));

        let timeText;
        if (diffMinutes < 1) {
            timeText = '刚刚';
        } else if (diffMinutes < 60) {
            timeText = `${diffMinutes}分钟前`;
        } else {
            timeText = lastUpdate.toLocaleString();
        }

        document.getElementById('last-update').textContent = `最后更新: ${timeText}`;
    }
}

// 显示错误消息
function showErrorMessage(title, message) {
    const modal = document.getElementById('info-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');

    modalTitle.textContent = title;
    modalBody.innerHTML = `
        <div style="color: #721c24; background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 5px; padding: 15px; margin: 10px 0;">
            <h4 style="margin-top: 0; color: #721c24;">${title}</h4>
            <div style="margin-bottom: 15px;">${message}</div>
            <div style="display: flex; gap: 10px;">
                <button onclick="location.reload()" style="padding: 8px 16px; background: #e74c3c; color: white; border: none; border-radius: 3px; cursor: pointer;">刷新页面</button>
                <button onclick="document.getElementById('info-modal').style.display='none'" style="padding: 8px 16px; background: #6c757d; color: white; border: none; border-radius: 3px; cursor: pointer;">关闭</button>
            </div>
        </div>
    `;

    modal.style.display = 'block';
}

// 显示警告消息
function showWarningMessage(title, message) {
    const modal = document.getElementById('info-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');

    modalTitle.textContent = title;
    modalBody.innerHTML = `
        <div style="color: #856404; background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 5px; padding: 15px; margin: 10px 0;">
            <h4 style="margin-top: 0; color: #856404;">${title}</h4>
            <div style="margin-bottom: 15px;">${message}</div>
            <button onclick="document.getElementById('info-modal').style.display='none'" style="padding: 8px 16px; background: #f39c12; color: white; border: none; border-radius: 3px; cursor: pointer;">确定</button>
        </div>
    `;

    modal.style.display = 'block';
}

// 显示成功消息
function showSuccessMessage(title, message) {
    const modal = document.getElementById('info-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');

    modalTitle.textContent = title;
    modalBody.innerHTML = `
        <div style="color: #155724; background-color: #d4edda; border: 1px solid #c3e6cb; border-radius: 5px; padding: 15px; margin: 10px 0;">
            <h4 style="margin-top: 0; color: #155724;">${title}</h4>
            <div style="margin-bottom: 15px;">${message}</div>
            <button onclick="document.getElementById('info-modal').style.display='none'" style="padding: 8px 16px; background: #27ae60; color: white; border: none; border-radius: 3px; cursor: pointer;">确定</button>
        </div>
    `;

    modal.style.display = 'block';
}

// 辅助函数：格式化数字
function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

// 辅助函数：格式化时间
function formatTime(dateString) {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) {
        return '刚刚';
    } else if (diffMins < 60) {
        return `${diffMins}分钟前`;
    } else if (diffHours < 24) {
        return `${diffHours}小时前`;
    } else if (diffDays < 7) {
        return `${diffDays}天前`;
    } else {
        return date.toLocaleDateString();
    }
}

// 加载初始数据（用于手动触发）
async function loadInitialData() {
    await checkBackendAndLoadData();
}

// 导出全局函数
window.SDFS = {
    loadData: loadInitialData,
    updateData: () => document.getElementById('update-data-btn').click(),
    resetView: () => mapVisualization.resetView(),
    testAISDecoding,
    showDetailedAISResults,
    exportAISResults,
    copyAISResults,
    runFullAISDecode,
    dataHandler,
    mapVisualization,
    toggleDebugPanel
};

console.log('SDFS前端模块加载完成');