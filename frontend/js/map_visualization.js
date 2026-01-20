// 地图可视化模块
class MapVisualization {
    constructor() {
        this.map = null;
        this.markersLayer = null;
        this.coverageLayers = null;
        this.aisMarkers = new Map();
        this.adsbMarkers = new Map();
        this.coveragePolygons = new Map();
        this.currentZoom = 8;
        this.mapCenter = [39.9042, 116.4074]; // 北京
    }

    // 初始化地图
    initializeMap() {
        // 创建地图实例
        this.map = L.map('map', {
            center: this.mapCenter,
            zoom: this.currentZoom,
            attributionControl: true
        });

        // 添加OpenStreetMap底图
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(this.map);

        // 添加卫星图选项
        const satelliteLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
            attribution: '© <a href="https://www.esri.com/">Esri</a>'
        });

        // 添加地图图层控制
        const baseLayers = {
            "标准地图": L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '© OpenStreetMap contributors'
            }),
            "卫星图像": satelliteLayer
        };

        // 添加图层控制
        L.control.layers(baseLayers).addTo(this.map);

        // 初始化图层组
        this.markersLayer = L.layerGroup().addTo(this.map);
        this.coverageLayers = L.layerGroup().addTo(this.map);

        // 添加比例尺
        L.control.scale({ imperial: false }).addTo(this.map);

        // 添加鼠标位置显示
        this.map.on('mousemove', (e) => {
            const coords = e.latlng;
            document.getElementById('coordinate-display').textContent =
                `坐标: ${coords.lat.toFixed(6)}, ${coords.lng.toFixed(6)}`;
        });

        // 监听地图视图变化
        this.map.on('zoomend', () => {
            this.currentZoom = this.map.getZoom();
        });

        console.log('地图初始化完成');
        return this.map;
    }

    // 创建AIS标记
    // 在createAisMarker和createAdsbMarker方法中添加数据状态指示
    createAisMarker(aisData, index) {
        const isOnline = dataHandler.isDataPointOnline(aisData.timestamp);

        // 根据数据状态选择颜色
        let color;
        if (aisData.data_status === 'error') {
            color = '#ff0000'; // 红色表示错误数据
        } else if (aisData.data_status === 'warning') {
            color = '#ff9900'; // 橙色表示待复核数据
        } else {
            color = isOnline ? '#3498db' : '#95a5a6'; // 正常数据
        }

        // 创建自定义图标
        const aisIcon = L.divIcon({
            className: 'ais-marker',
            html: `
                <div style="
                    width: 15px;
                    height: 15px;
                    background-color: ${color};
                    border: 2px solid white;
                    border-radius: 50%;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.3);
                    position: relative;
                ">
                    ${aisData.data_status === 'warning' ? `
                        <div style="
                            position: absolute;
                            top: -3px;
                            right: -3px;
                            width: 6px;
                            height: 6px;
                            background-color: #ff9900;
                            border-radius: 50%;
                            border: 1px solid white;
                        "></div>
                    ` : ''}
                    ${aisData.data_status === 'error' ? `
                        <div style="
                            position: absolute;
                            top: -3px;
                            right: -3px;
                            width: 6px;
                            height: 6px;
                            background-color: #ff0000;
                            border-radius: 50%;
                            border: 1px solid white;
                        "></div>
                    ` : ''}
                </div>
            `,
            iconSize: [19, 19],
            iconAnchor: [9, 9]
        });

        // 创建标记
        const marker = L.marker([aisData.latitude, aisData.longitude], {
            icon: aisIcon,
            title: `船舶: ${aisData.mmsi} (${aisData.data_status === 'normal' ? '正常' :
                                          aisData.data_status === 'warning' ? '待复核' : '错误'})`
        });

        // 添加弹出窗口
        const popupContent = this.createAisPopupContent(aisData);
        marker.bindPopup(popupContent);

        // 添加点击事件
        marker.on('click', () => {
            this.showDataPointInfo(aisData, 'ais');
        });

        return marker;
    }

    createAdsbMarker(adsbData, index) {
        const isOnline = dataHandler.isDataPointOnline(adsbData.timestamp);

        // 根据数据状态选择颜色
        let color;
        if (adsbData.data_status === 'error') {
            color = '#ff0000'; // 红色表示错误数据
        } else if (adsbData.data_status === 'warning') {
            color = '#ff9900'; // 橙色表示待复核数据
        } else {
            color = isOnline ? '#e74c3c' : '#95a5a6'; // 正常数据
        }

        // 创建自定义图标（飞机形状）
        const adsbIcon = L.divIcon({
            className: 'adsb-marker',
            html: `
                <div style="
                    width: 20px;
                    height: 20px;
                    background-color: ${color};
                    border: 2px solid white;
                    border-radius: 3px;
                    transform: rotate(45deg);
                    box-shadow: 0 2px 4px rgba(0,0,0,0.3);
                    position: relative;
                ">
                    <div style="
                        position: absolute;
                        top: -4px;
                        left: 7px;
                        width: 6px;
                        height: 6px;
                        background-color: white;
                        border-radius: 50%;
                    "></div>
                    ${adsbData.data_status === 'warning' ? `
                        <div style="
                            position: absolute;
                            top: -5px;
                            right: -5px;
                            width: 8px;
                            height: 8px;
                            background-color: #ff9900;
                            border-radius: 50%;
                            border: 1px solid white;
                            transform: rotate(-45deg);
                        "></div>
                    ` : ''}
                    ${adsbData.data_status === 'error' ? `
                        <div style="
                            position: absolute;
                            top: -5px;
                            right: -5px;
                            width: 8px;
                            height: 8px;
                            background-color: #ff0000;
                            border-radius: 50%;
                            border: 1px solid white;
                            transform: rotate(-45deg);
                        "></div>
                    ` : ''}
                </div>
            `,
            iconSize: [24, 24],
            iconAnchor: [12, 12]
        });

        // 创建标记
        const marker = L.marker([adsbData.latitude, adsbData.longitude], {
            icon: adsbIcon,
            title: `飞机: ${adsbData.aircraft_tail} (${adsbData.data_status === 'normal' ? '正常' :
                                                adsbData.data_status === 'warning' ? '待复核' : '错误'})`
        });

        // 添加弹出窗口
        const popupContent = this.createAdsbPopupContent(adsbData);
        marker.bindPopup(popupContent);

        // 添加点击事件
        marker.on('click', () => {
            this.showDataPointInfo(adsbData, 'adsb');
        });

        return marker;
    }

    // 在弹出窗口中添加数据状态信息
    createAisPopupContent(aisData) {
        const isOnline = dataHandler.isDataPointOnline(aisData.timestamp);

        // 判断是否为CSV格式（包含额外字段）
        const isCsvFormat = aisData.vessel_name !== undefined && aisData.vessel_name !== 'unknown';

        // 数据状态标签
        let statusLabel = '';
        let statusColor = '';
        let statusBg = '';

        if (aisData.data_status === 'normal') {
            statusLabel = '正常';
            statusColor = '#27ae60';
            statusBg = '#d4edda';
        } else if (aisData.data_status === 'warning') {
            statusLabel = '待复核';
            statusColor = '#ff9900';
            statusBg = '#fff3cd';
        } else {
            statusLabel = '错误';
            statusColor = '#e74c3c';
            statusBg = '#f8d7da';
        }

        return `
            <div style="min-width: 250px; font-family: Arial, sans-serif;">
                <div style="
                    background-color: ${isOnline ? '#3498db' : '#95a5a6'};
                    color: white;
                    padding: 5px 10px;
                    border-radius: 3px 3px 0 0;
                    margin: -10px -10px 10px -10px;
                    font-weight: bold;
                ">
                    船舶信息 (AIS${isCsvFormat ? ' - CSV格式' : ' - NMEA格式'})
                </div>
                <div style="
                    background-color: ${statusBg};
                    color: ${statusColor};
                    padding: 3px 8px;
                    margin: 5px 0;
                    border-radius: 12px;
                    font-size: 0.8rem;
                    font-weight: bold;
                    display: inline-block;
                ">
                    ${statusLabel}
                </div>
                ${aisData.cleaning_notes ? `
                    <div style="
                        background-color: #f8f9fa;
                        border-left: 3px solid ${statusColor};
                        padding: 5px 8px;
                        margin: 5px 0;
                        font-size: 0.8rem;
                        color: #6c757d;
                    ">
                        <strong>数据质量备注:</strong> ${aisData.cleaning_notes}
                    </div>
                ` : ''}
                <p><strong>MMSI:</strong> ${aisData.mmsi}</p>
                ${isCsvFormat ? `
                    <p><strong>船名:</strong> ${aisData.vessel_name || '未知'}</p>
                    <p><strong>呼号:</strong> ${aisData.call_sign || '未知'}</p>
                    <p><strong>IMO:</strong> ${aisData.imo || '未知'}</p>
                ` : ''}
                <p><strong>位置:</strong><br>
                   ${aisData.latitude.toFixed(6)}°N,<br>
                   ${aisData.longitude.toFixed(6)}°E</p>
                <p><strong>航速:</strong> ${aisData.sog.toFixed(1)} 节</p>
                <p><strong>航向:</strong> ${aisData.cog.toFixed(1)}°</p>
                <p><strong>船舶类型:</strong> ${aisData.vessel_type}</p>
                <p><strong>航行状态:</strong> ${aisData.nav_status}</p>
                ${isCsvFormat ? `
                    ${aisData.length > 0 ? `<p><strong>尺寸:</strong> ${aisData.length.toFixed(1)}×${aisData.width.toFixed(1)}m</p>` : ''}
                    ${aisData.draft > 0 ? `<p><strong>吃水:</strong> ${aisData.draft.toFixed(1)}m</p>` : ''}
                    ${aisData.cargo && aisData.cargo !== 'unknown' ? `<p><strong>货物:</strong> ${aisData.cargo}</p>` : ''}
                ` : ''}
                <p><strong>在线状态:</strong>
                    <span style="color: ${isOnline ? '#27ae60' : '#e74c3c'}">
                        ${isOnline ? '在线' : '离线'}
                    </span>
                </p>
                <p><strong>时间:</strong> ${new Date(aisData.timestamp).toLocaleString()}</p>
                <p><small>点击标记查看详细信息</small></p>
            </div>
        `;
    }

    createAdsbPopupContent(adsbData) {
        const isOnline = dataHandler.isDataPointOnline(adsbData.timestamp);

        // 数据状态标签
        let statusLabel = '';
        let statusColor = '';
        let statusBg = '';

        if (adsbData.data_status === 'normal') {
            statusLabel = '正常';
            statusColor = '#27ae60';
            statusBg = '#d4edda';
        } else if (adsbData.data_status === 'warning') {
            statusLabel = '待复核';
            statusColor = '#ff9900';
            statusBg = '#fff3cd';
        } else {
            statusLabel = '错误';
            statusColor = '#e74c3c';
            statusBg = '#f8d7da';
        }

        return `
            <div style="min-width: 250px; font-family: Arial, sans-serif;">
                <div style="
                    background-color: ${isOnline ? '#e74c3c' : '#95a5a6'};
                    color: white;
                    padding: 5px 10px;
                    border-radius: 3px 3px 0 0;
                    margin: -10px -10px 10px -10px;
                    font-weight: bold;
                ">
                    飞机信息 (ADS-B)
                </div>
                <div style="
                    background-color: ${statusBg};
                    color: ${statusColor};
                    padding: 3px 8px;
                    margin: 5px 0;
                    border-radius: 12px;
                    font-size: 0.8rem;
                    font-weight: bold;
                    display: inline-block;
                ">
                    ${statusLabel}
                </div>
                ${adsbData.cleaning_notes ? `
                    <div style="
                        background-color: #f8f9fa;
                        border-left: 3px solid ${statusColor};
                        padding: 5px 8px;
                        margin: 5px 0;
                        font-size: 0.8rem;
                        color: #6c757d;
                    ">
                        <strong>数据质量备注:</strong> ${adsbData.cleaning_notes}
                    </div>
                ` : ''}
                <p><strong>飞机ID:</strong> ${adsbData.aircraft_id}</p>
                <p><strong>尾号:</strong> ${adsbData.aircraft_tail}</p>
                <p><strong>位置:</strong><br>
                   ${adsbData.latitude.toFixed(6)}°N,<br>
                   ${adsbData.longitude.toFixed(6)}°E</p>
                <p><strong>高度:</strong> ${adsbData.altitude_ft.toFixed(0)} 英尺</p>
                <p><strong>地速:</strong> ${adsbData.ground_speed_kts.toFixed(0)} 节</p>
                <p><strong>航向:</strong> ${adsbData.heading_deg.toFixed(1)}°</p>
                <p><strong>在线状态:</strong>
                    <span style="color: ${isOnline ? '#27ae60' : '#e74c3c'}">
                        ${isOnline ? '在线' : '离线'}
                    </span>
                </p>
                <p><strong>时间:</strong> ${new Date(adsbData.timestamp).toLocaleString()}</p>
                <p><small>点击标记查看详细信息</small></p>
            </div>
        `;
    }

    // 创建覆盖范围图层
    createCoverageLayer(coverageData) {
        if (!coverageData.coordinates || coverageData.coordinates.length < 3) {
            console.warn('无效的覆盖范围数据:', coverageData);
            return null;
        }

        // 将坐标转换为Leaflet格式
        const latLngs = coverageData.coordinates.map(coord => [coord[1], coord[0]]);

        // 创建多边形
        const polygon = L.polygon(latLngs, {
            color: coverageData.data_type === 'ais' ? '#3498db' : '#e74c3c',
            weight: 2,
            opacity: 0.8,
            fillColor: coverageData.data_type === 'ais' ? '#3498db' : '#e74c3c',
            fillOpacity: 0.2,
            className: 'coverage-layer'
        });

        // 添加弹出窗口
        const popupContent = this.createCoveragePopupContent(coverageData);
        polygon.bindPopup(popupContent);

        // 添加点击事件
        polygon.on('click', () => {
            this.showCoverageLayerInfo(coverageData);
        });

        return polygon;
    }

    // 创建覆盖范围弹出窗口内容
    createCoveragePopupContent(coverageData) {
        return `
            <div style="min-width: 200px; font-family: Arial, sans-serif;">
                <div style="
                    background-color: ${coverageData.data_type === 'ais' ? '#3498db' : '#e74c3c'};
                    color: white;
                    padding: 5px 10px;
                    border-radius: 3px 3px 0 0;
                    margin: -10px -10px 10px -10px;
                    font-weight: bold;
                ">
                    ${coverageData.label}
                </div>
                <p><strong>数据类型:</strong> ${coverageData.data_type.toUpperCase()}</p>
                <p><strong>状态:</strong>
                    <span style="color: ${coverageData.status === 'online' ? '#27ae60' : '#e74c3c'}">
                        ${coverageData.status === 'online' ? '在线' : '离线'}
                    </span>
                </p>
                <p><strong>坐标点数:</strong> ${coverageData.coordinates.length}</p>
                <p><strong>更新时间:</strong><br>
                   ${new Date(coverageData.metadata?.update_time).toLocaleString()}</p>
                <p><small>点击区域查看详细信息</small></p>
            </div>
        `;
    }

    // 显示数据点详细信息
    showDataPointInfo(dataPoint, dataType) {
        const modal = document.getElementById('info-modal');
        const modalTitle = document.getElementById('modal-title');
        const modalBody = document.getElementById('modal-body');

        modalTitle.textContent = dataType === 'ais' ? '船舶详细信息 (AIS)' : '飞机详细信息 (ADS-B)';
        modalBody.innerHTML = dataHandler.formatDataPointInfo(dataPoint);

        modal.style.display = 'block';
    }

    // 显示覆盖层信息
    showCoverageLayerInfo(coverageLayer) {
        const modal = document.getElementById('info-modal');
        const modalTitle = document.getElementById('modal-title');
        const modalBody = document.getElementById('modal-body');

        modalTitle.textContent = '覆盖范围详细信息';
        modalBody.innerHTML = dataHandler.formatCoverageLayerInfo(coverageLayer);

        modal.style.display = 'block';
    }

    // 更新地图数据
    updateMapData(aisData, adsbData, coverageLayers) {
        // 清除现有标记和图层
        this.clearMap();

        console.log(`开始更新地图数据: AIS=${aisData.length}, ADS-B=${adsbData.length}, 覆盖层=${coverageLayers.length}`);

        // 添加AIS标记
        if (aisData && aisData.length > 0) {
            aisData.forEach((ais, index) => {
                if (this.shouldShowAis()) {
                    const marker = this.createAisMarker(ais, index);
                    marker.addTo(this.markersLayer);
                    this.aisMarkers.set(`ais_${index}`, marker);
                }
            });
            console.log(`添加了 ${this.aisMarkers.size} 个AIS标记`);
        }

        // 添加ADS-B标记
        if (adsbData && adsbData.length > 0) {
            adsbData.forEach((adsb, index) => {
                if (this.shouldShowAdsb()) {
                    const marker = this.createAdsbMarker(adsb, index);
                    marker.addTo(this.markersLayer);
                    this.adsbMarkers.set(`adsb_${index}`, marker);
                }
            });
            console.log(`添加了 ${this.adsbMarkers.size} 个ADS-B标记`);
        }

        // 添加覆盖范围图层
        if (coverageLayers && coverageLayers.length > 0) {
            coverageLayers.forEach((layer, index) => {
                if (this.shouldShowCoverage()) {
                    const polygon = this.createCoverageLayer(layer);
                    if (polygon) {
                        polygon.addTo(this.coverageLayers);
                        this.coveragePolygons.set(`coverage_${index}`, polygon);
                    }
                }
            });
            console.log(`添加了 ${this.coveragePolygons.size} 个覆盖范围图层`);
        }

        // 如果添加了标记，调整地图视图
        if (this.aisMarkers.size > 0 || this.adsbMarkers.size > 0) {
            this.fitMapToBounds();
        }

        console.log('地图数据更新完成');
    }

    // 清除地图上的所有标记和图层
    clearMap() {
        // 清除标记
        this.aisMarkers.forEach(marker => this.markersLayer.removeLayer(marker));
        this.adsbMarkers.forEach(marker => this.markersLayer.removeLayer(marker));
        this.coveragePolygons.forEach(polygon => this.coverageLayers.removeLayer(polygon));

        // 清空集合
        this.aisMarkers.clear();
        this.adsbMarkers.clear();
        this.coveragePolygons.clear();

        console.log('地图已清除');
    }

    // 调整地图视图以适应所有标记
    fitMapToBounds() {
        const bounds = [];

        // 收集所有标记的边界
        this.aisMarkers.forEach(marker => {
            bounds.push(marker.getLatLng());
        });

        this.adsbMarkers.forEach(marker => {
            bounds.push(marker.getLatLng());
        });

        // 如果有标记，调整视图
        if (bounds.length > 0) {
            const latLngBounds = L.latLngBounds(bounds);
            this.map.fitBounds(latLngBounds, { padding: [50, 50] });
        }
    }

    // 重置地图视图
    resetView() {
        this.map.setView(this.mapCenter, this.currentZoom);
        console.log('地图视图已重置');
    }

    // 过滤检查
    shouldShowAis() {
        return document.getElementById('filter-ais').checked;
    }

    shouldShowAdsb() {
        return document.getElementById('filter-adsb').checked;
    }

    shouldShowCoverage() {
        return document.getElementById('filter-coverage').checked;
    }

    // 根据过滤设置更新可见性
    updateVisibility() {
        // 更新AIS标记可见性
        this.aisMarkers.forEach((marker, key) => {
            if (this.shouldShowAis()) {
                this.markersLayer.addLayer(marker);
            } else {
                this.markersLayer.removeLayer(marker);
            }
        });

        // 更新ADS-B标记可见性
        this.adsbMarkers.forEach((marker, key) => {
            if (this.shouldShowAdsb()) {
                this.markersLayer.addLayer(marker);
            } else {
                this.markersLayer.removeLayer(marker);
            }
        });

        // 更新覆盖范围可见性
        this.coveragePolygons.forEach((polygon, key) => {
            if (this.shouldShowCoverage()) {
                this.coverageLayers.addLayer(polygon);
            } else {
                this.coverageLayers.removeLayer(polygon);
            }
        });

        console.log('地图可见性已更新');
    }
}

// 创建全局地图可视化实例
const mapVisualization = new MapVisualization();