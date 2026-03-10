const { useState, useEffect, useRef, useMemo } = React;
const {
    Layout, Menu, Button, Card, Table, Select, Input, Tag,
    Space, Row, Col, Statistic, Alert, Tabs, Badge, Spin, message,
    Segmented, Typography, Divider, Progress, Modal
} = antd;
const {
    DashboardOutlined, LineChartOutlined,
    BookOutlined, TeamOutlined, EnvironmentOutlined, RiseOutlined,
    FallOutlined, MinusOutlined, BankOutlined  // 添加 BankOutlined 作为场站图标
} = icons;

const { Header, Content } = Layout;
const { Title, Text } = Typography;

const API_BASE_URL = '/api/pxxdash';

// 重点35城列表
const KEY_35_CITIES = [
    '无锡市', '合肥市', '武汉市', '苏州市', '南京市', '常州市', '上海市', '厦门市', '福州市', '泉州市',
    '长沙市', '南昌市', '杭州市', '宁波市', '温州市', '金华市', '绍兴市', '嘉兴市', '台州市', '重庆市',
    '成都市', '西安市', '深圳市', '广州市', '佛山市', '东莞市', '南宁市', '昆明市', '沈阳市', '北京市',
    '大连市', '天津市', '郑州市', '青岛市', '济南市'
];

// // 季度配置
// const QUARTERS = [
//     { key: 'q1_2024', label: '2024Q1', field: '2024年一季度财务模型' },
//     { key: 'q2_2024', label: '2024Q2', field: '2024年二季度财务模型' },
//     { key: 'q3_2024', label: '2024Q3', field: '2024年三季度财务模型' },
//     { key: 'q4_2024', label: '2024Q4', field: '2024年四季度财务模型' },
//     { key: 'q1_2025', label: '2025Q1', field: '2025年一季度财务模型' },
//     { key: 'q2_2025', label: '2025Q2', field: '2025年二季度财务模型' },
//     { key: 'q3_2025', label: '2025Q3', field: '2025年三季度财务模型' }
// ];

// 主应用组件
const App = () => {
    // 状态管理
    const [currentPage, setCurrentPage] = useState('guide');
    const [loading, setLoading] = useState(false);
    const [rawData, setRawData] = useState([]);
    const [invalidDataCount, setInvalidDataCount] = useState(0);
    const [alertDismissed, setAlertDismissed] = useState(false);
    const [dataLoading, setDataLoading] = useState(false);
    const [availableQuarters, setAvailableQuarters] = useState([]);

    // 图表引用
    const trendChartRef = useRef(null);
    const regionChartRef = useRef(null);
    const provinceChartRef = useRef(null);

    // 图表实例
    const [charts, setCharts] = useState({
        trend: null,
        region: null,
        province: null
    });

    // 当前季度信息
    const [quarterInfo, setQuarterInfo] = useState({
        current: { label: '', field: '' },
        previous: { label: '', field: '' }
    });

    // 动态季度信息
    const [QUARTERS, setQUARTERS] = useState([]);
    const [quartersLoaded, setQuartersLoaded] = useState(false);

    // 获取可用季度列表
    const fetchAvailableQuarters = async () => {
        try {
            const response = await fetch(`${API_BASE_URL}/quarters`);
            const result = await response.json();
            if (result.success) {
                setAvailableQuarters(result.quarters);

                // 设置动态季度配置
                if (result.quartersConfig && result.quartersConfig.length > 0) {
                    setQUARTERS(result.quartersConfig);
                    setQuartersLoaded(true);
                }
            }
        } catch (error) {
            console.error('Failed to fetch quarters:', error);
            message.error('获取季度配置失败');
        }
    };

    // 从API获取数据
    const fetchDashboardData = async (selectedQuarters = null) => {
        setDataLoading(true);
        try {
            // 构建查询参数
            let url = `${API_BASE_URL}/data`;
            if (selectedQuarters && selectedQuarters.length > 0) {
                const params = new URLSearchParams();
                selectedQuarters.forEach(q => params.append('quarters', q));
                url += '?' + params.toString();
            }

            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const result = await response.json();
            if (result.success) {
                // 转换数据格式以匹配原有的字段名
                const transformedData = result.data.map(item => {
                    const baseData = {
                        '大区': item.region,
                        '省份': item.province,
                        '城市': item.city,
                        '项目类型': item.project_type,
                        '网络发展人员': item.bd,
                        '场站类型（2C/4C/5C）': item.product_type,
                        '枪数': item.gun_count,
                        '项目编号': item.project_id,
                        '项目名称': item.project_name,
                        '上会-财务模型': item.approved_model,
                        '累计-财务模型': item.cumulative_model,
                    };

                    // 动态添加所有季度字段
                    Object.keys(item).forEach(key => {
                        if (key.includes('财务模型') && !key.includes('approved') && !key.includes('cumulative')) {
                            baseData[key] = item[key];
                        }
                    });

                    return baseData;
                });

                setRawData(transformedData);
                detectCurrentQuarter(transformedData);
                message.success(`数据加载成功！共 ${transformedData.length} 个项目`);
            } else {
                throw new Error('API返回失败状态');
            }
        } catch (error) {
            message.error('数据加载失败：' + error.message);
            console.error('Fetch error:', error);
        } finally {
            setDataLoading(false);
        }
    };

    // 组件加载时先获取季度信息
    useEffect(() => {
        fetchAvailableQuarters();
    }, []);

    // 季度信息后加载数据
    useEffect(() => {
        if (quartersLoaded && QUARTERS.length > 0) {
            fetchDashboardData();
        }
    }, [quartersLoaded, QUARTERS]);

    // 检测当前季度
    const detectCurrentQuarter = (data) => {
        // 添加空值检查
        if (!QUARTERS || QUARTERS.length === 0) {
            console.warn('QUARTERS not loaded yet');
            return;
        }

        for (let i = QUARTERS.length - 1; i >= 0; i--) {
            const quarter = QUARTERS[i];
            const hasData = data.some(item => item[quarter.field]);
            if (hasData) {
                setQuarterInfo({
                    current: { label: quarter.label, field: quarter.field },
                    previous: i > 0 ? {
                        label: QUARTERS[i - 1].label,
                        field: QUARTERS[i - 1].field
                    } : { label: '', field: '' }
                });
                break;
            }
        }
    };


    // 使用指南组件
    const GuideView = () => (
        <div>
            <Card title={<><BookOutlined /> 上会财务模型计算逻辑</>} style={{ marginBottom: 16 }}>
                <p><strong>定价得分基于与城市均价的差价：</strong></p>
                <ul>
                    <li>差价 ≤ 0元时：得50分</li>
                    <li>0 &lt; 差价 ≤ 0.2元：得40分</li>
                    <li>0.2 &lt; 差价 ≤ 0.3元：得30分</li>
                    <li>0.3 &lt; 差价 ≤ 0.4元：得20分</li>
                    <li>0.4 &lt; 差价 ≤ 0.5元：得10分</li>
                    <li>差价 &gt; 0.5元：得0分</li>
                </ul>

                <Divider />

                <p><strong>三年内平均单枪充电量得分规则：</strong></p>
                <ul>
                    <li>≥ 200度：得50分</li>
                    <li>充电量每下降40度，得分减少10分</li>
                </ul>

                <Alert
                    message="总分等级划分"
                    description="≥90分 为 P10 | 70（含）-90分 为 P30 | 50（含）-70分 为 P50 | ≤50分 为 P90"
                    type="info"
                    showIcon
                    style={{ marginTop: 16 }}
                />
            </Card>

            <Card title={<><LineChartOutlined /> 上线季度财务结果计算逻辑</>} style={{ marginBottom: 16 }}>
                <p><strong>达成率计分规则：</strong></p>
                <ul>
                    <li>"运营充电量（季度平均）/上会首年充电量" & "运营定价（季度平均）/上会定价"</li>
                    <li>根据"达成率"进行对称式阶梯计分：</li>
                    <li>达成率为100%时：得0分</li>
                    <li>每向上提升10%，得分增加10分（最高150%及以上得50分）</li>
                    <li>每向下降低10%，得分减少10分（最低60%以下得-50分）</li>
                </ul>

                <Alert
                    message="总分计算"
                    description="总分 = 上线季度财务得分 + 上会财务得分"
                    type="success"
                    showIcon
                    style={{ marginTop: 16 }}
                />
            </Card>
        </div>
    );

    // 概览看板组件
    const DashboardView = ({ currentPage, QUARTERS }) => {
        const [quarterComparison, setQuarterComparison] = useState({});
        // 新增：控制图表数据源的状态 (false: 当季度, true: 累计)
        const [useCumulative, setUseCumulative] = useState(false);
        // 新增：累计数据的统计状态
        const [cumulativeStats, setCumulativeStats] = useState({ P10: 0, P30: 0, P50: 0, P90: 0, total: 0 });

        // 使用 useRef 存储图表实例
        const chartInstanceRef = useRef(null);
        const regionChartRef = useRef(null);
        const provinceChartRef = useRef(null);
        const regionChartInstance = useRef(null);
        const provinceChartInstance = useRef(null);

        // 计算累计数据统计
        useEffect(() => {
            if (rawData.length > 0) {
                const stats = { P10: 0, P30: 0, P50: 0, P90: 0, total: 0 };
                rawData.forEach(item => {
                    const level = item['累计-财务模型'];
                    if (level && stats.hasOwnProperty(level)) {
                        stats[level]++;
                        stats.total++;
                    }
                });
                setCumulativeStats(stats);
            }
        }, [rawData]);

        useEffect(() => {
            if (rawData.length > 0 && quarterInfo.current.field && quarterInfo.previous.field) {
                calculateQuarterComparison();
            }
        }, [rawData, quarterInfo]);

        // 处理趋势图表初始化
        useEffect(() => {
            if (rawData.length > 0 && trendChartRef.current && QUARTERS && QUARTERS.length > 0) {
                if (chartInstanceRef.current) {
                    chartInstanceRef.current.dispose();
                }

                const chart = echarts.init(trendChartRef.current);
                chartInstanceRef.current = chart;

                updateTrendChart(chart);

                const handleResize = () => chart.resize();
                window.addEventListener('resize', handleResize);

                return () => {
                    window.removeEventListener('resize', handleResize);
                    chart.dispose();
                };
            }
        }, [rawData, QUARTERS]);

        // 处理大区图表初始化
        useEffect(() => {
            if (rawData.length > 0 && regionChartRef.current && quarterInfo.current.field) {
                if (regionChartInstance.current) regionChartInstance.current.dispose();
                const chart = echarts.init(regionChartRef.current);
                regionChartInstance.current = chart;

                updateRegionChart(chart);

                const handleResize = () => chart.resize();
                window.addEventListener('resize', handleResize);

                return () => {
                    window.removeEventListener('resize', handleResize);
                    chart.dispose();
                };
            }
        }, [rawData, quarterInfo.current.field, QUARTERS, useCumulative]); // 添加 useCumulative 依赖

        // 修改：省份图表依赖 useCumulative
        useEffect(() => {
            if (rawData.length > 0 && provinceChartRef.current && quarterInfo.current.field) {
                if (provinceChartInstance.current) provinceChartInstance.current.dispose();
                const chart = echarts.init(provinceChartRef.current);
                provinceChartInstance.current = chart;
                updateProvinceChart(chart); // 内部会读取 useCumulative
                const handleResize = () => chart.resize();
                window.addEventListener('resize', handleResize);
                return () => {
                    window.removeEventListener('resize', handleResize);
                    chart.dispose();
                };
            }
        }, [rawData, quarterInfo.current.field, QUARTERS, useCumulative]); // 添加 useCumulative 依赖

        // 监听标签页切换
        useEffect(() => {
            if (currentPage === 'dashboard') {
                requestAnimationFrame(() => {
                    if (chartInstanceRef.current) chartInstanceRef.current.resize();
                    if (regionChartInstance.current) regionChartInstance.current.resize();
                    if (provinceChartInstance.current) provinceChartInstance.current.resize();
                });
            }
        }, [currentPage]);

        // 获取等级颜色
        const getLevelColor = (level) => {
            const colors = { 'P10': '#006400', 'P30': '#95de64', 'P50': '#ffa940', 'P90': '#ff4d4f' };
            return colors[level] || '#666';
        };

        // 修改：updateRegionChart 支持切换数据源
        const updateRegionChart = (chart) => {
            if (!chart || !quarterInfo.current.field) return;

            // 确定使用的字段
            const targetField = useCumulative ? '累计-财务模型' : quarterInfo.current.field;
            const titleSuffix = useCumulative ? '累计' : quarterInfo.current.label;

            const regionData = {};
            rawData.forEach(item => {
                const region = item['大区'];
                const level = item[targetField]; // 使用动态字段
                if (region && level) {
                    if (!regionData[region]) {
                        regionData[region] = { P10: 0, P30: 0, P50: 0, P90: 0, total: 0 };
                    }
                    if (regionData[region].hasOwnProperty(level)) {
                        regionData[region][level]++;
                    }
                    regionData[region].total++;
                }
            });

            const regions = Object.keys(regionData).sort();
            const levels = ['P10', 'P30', 'P50', 'P90'];

            const series = levels.map(level => ({
                name: level,
                type: 'bar',
                stack: 'total',
                barWidth: '40%',
                emphasis: {
                    focus: 'series'
                },
                data: regions.map(r => regionData[r][level] || 0),
                itemStyle: { color: getLevelColor(level) },
                label: {
                    show: true,
                    position: 'inside',
                    formatter: (params) => {
                        const value = params.value;
                        const total = regionData[params.name].total;
                        const percentage = total > 0 ? Math.round(value / total * 100) : 0;
                        return value > 0 ? `${value}(${percentage}%)` : '';
                    }
                }
            }));

            const option = {
                title: { text: `各大区财务等级分布 (${titleSuffix})`, left: 'center' },
                tooltip: {
                    trigger: 'axis',
                    axisPointer: { type: 'shadow' },
                    formatter: (params) => {
                        const region = params[0].name;
                        const data = regionData[region];
                        let result = `${region}<br/>`;
                        // 排序显示 tooltip
                        const sortedParams = params.sort((a, b) => {
                            const order = { 'P90': 0, 'P50': 1, 'P30': 2, 'P10': 3 };
                            return order[a.seriesName] - order[b.seriesName];
                        });
                        sortedParams.forEach(param => {
                            const percentage = data.total > 0 ? Math.round(param.value / data.total * 100) : 0;
                            result += `${param.marker} ${param.seriesName}: ${param.value} (${percentage}%)<br/>`;
                        });
                        result += `<div style="font-weight: bold; margin-top: 4px;">总计: ${data.total} </div>`;
                        return result;
                    }
                },
                legend: { data: levels, bottom: 0 },
                grid: { left: '3%', right: '4%', bottom: '10%', top: '10%', containLabel: true },
                xAxis: { type: 'category', data: regions },
                yAxis: { type: 'value', name: '场站数' },
                series: series
            };
            chart.setOption(option);
        };

        // 修改：updateProvinceChart 支持切换数据源
        const updateProvinceChart = (chart) => {
            if (!chart || !quarterInfo.current.field) return;

            // 确定使用的字段
            const targetField = useCumulative ? '累计-财务模型' : quarterInfo.current.field;
            const titleSuffix = useCumulative ? '累计' : quarterInfo.current.label;

            const provinceData = {};
            rawData.forEach(item => {
                const province = item['省份'];
                const level = item[targetField]; // 使用动态字段
                if (province && level) {
                    if (!provinceData[province]) {
                        provinceData[province] = { P10: 0, P30: 0, P50: 0, P90: 0, total: 0 };
                    }
                    if (provinceData[province].hasOwnProperty(level)) {
                        provinceData[province][level]++;
                    }
                    provinceData[province].total++;
                }
            });

            // 按总数排序all provinces
            const provinces = Object.keys(provinceData)
                .sort((a, b) => provinceData[b].total - provinceData[a].total)
                .reverse();

            const levels = ['P10', 'P30', 'P50', 'P90'];
            const series = levels.map(level => ({
                name: level,
                type: 'bar',
                stack: 'total',
                barWidth: '40%',
                emphasis: {
                    focus: 'series'
                },
                data: provinces.map(p => provinceData[p][level] || 0),
                itemStyle: { color: getLevelColor(level) }
            }));

            const option = {
                title: { text: `省份财务等级分布 (${titleSuffix})`, left: 'center' },
                tooltip: {
                    trigger: 'axis',
                    axisPointer: { type: 'shadow' },
                    formatter: (params) => {
                        const province = params[0].name;
                        const data = provinceData[province];
                        let result = `${province}<br/>`;
                        params.forEach(param => {
                            const percentage = data.total > 0 ? Math.round(param.value / data.total * 100) : 0;
                            result += `${param.marker} ${param.seriesName}: ${param.value} (${percentage}%)<br/>`;
                        });
                        result += `<div style="font-weight: bold; margin-top: 4px;">总计: ${data.total}</div>`;
                        return result;
                    }
                },
                legend: { data: levels, bottom: 0 },
                grid: { left: '15%', right: '5%', bottom: '10%', top: '10%', containLabel: true },
                xAxis: { type: 'value', name: '场站数' },
                yAxis: { type: 'category', data: provinces, axisLabel: { interval: 0 } },
                series: series
            };
            chart.setOption(option);
        };

        // ... (calculateQuarterComparison 和 updateTrendChart 保持不变) ...
        const calculateQuarterComparison = () => {
            if (!quarterInfo.current.field || !quarterInfo.previous.field || !QUARTERS || QUARTERS.length === 0) return;
            const levels = ['P10', 'P30', 'P50', 'P90'];
            const previous = { P10: 0, P30: 0, P50: 0, P90: 0, total: 0 };
            const current = { P10: 0, P30: 0, P50: 0, P90: 0, total: 0 };
            rawData.forEach(item => {
                const prevLevel = item[quarterInfo.previous.field];
                const currLevel = item[quarterInfo.current.field];
                if (prevLevel && previous[prevLevel] !== undefined) { previous[prevLevel]++; previous.total++; }
                if (currLevel && current[currLevel] !== undefined) { current[currLevel]++; current.total++; }
            });
            setQuarterComparison({ previous, current });
        };

        const updateTrendChart = (chart) => {
            if (!chart) return;

            // 添加对 QUARTERS 的检查
            if (!QUARTERS || QUARTERS.length === 0) {
                console.warn('QUARTERS not available for trend chart');
                return;
            }

            const quarterData = QUARTERS.map(quarter => {
                const levelCounts = { P10: 0, P30: 0, P50: 0, P90: 0 };
                let total = 0;

                rawData.forEach(item => {
                    const level = item[quarter.field];
                    if (level && levelCounts[level] !== undefined) { levelCounts[level]++; total++; }
                });
                const p10p30Ratio = total > 0 ? ((levelCounts.P10 + levelCounts.P30) / total * 100).toFixed(1) : 0;
                return { levelCounts, total, p10p30Ratio };
            });

            const option = {
                title: { text: '各季度财务等级分布趋势', left: 'center' },
                tooltip: {
                    trigger: 'axis', axisPointer: { type: 'cross' }, formatter: (params) => {
                        const dataIndex = params[0].dataIndex;
                        const data = quarterData[dataIndex];
                        let result = `${params[0].name}<br/>`;
                        ['P90', 'P50', 'P30', 'P10'].forEach(level => {
                            const count = data.levelCounts[level];
                            const percentage = data.total > 0 ? (count / data.total * 100).toFixed(1) : 0;
                            const color = getLevelColor(level);
                            result += `<span style="color: ${color}">●</span> ${level}: ${count} (${percentage}%)<br/>`;
                        });
                        const p10p30Count = data.levelCounts.P10 + data.levelCounts.P30;
                        result += `<br/><strong>P10+P30占比: ${data.p10p30Ratio}%</strong>`;
                        return result;
                    }
                },
                legend: { data: ['P10', 'P30', 'P50', 'P90', 'P10+P30占比'], bottom: 0 },
                grid: { left: '3%', right: '4%', bottom: '10%', top: '15%', containLabel: true },
                xAxis: { type: 'category', data: QUARTERS.map(q => q.label), boundaryGap: false },
                yAxis: [{ type: 'value', name: '场站数量', position: 'left' }, { type: 'value', name: 'P10+P30占比(%)', position: 'right', max: 100, axisLabel: { formatter: '{value}%' } }],
                series: [
                    { name: 'P10', type: 'line', stack: 'total', areaStyle: { color: '#006400' }, lineStyle: { color: '#006400' }, itemStyle: { color: '#006400' }, emphasis: { focus: 'series' }, data: quarterData.map(d => d.levelCounts.P10), yAxisIndex: 0 },
                    { name: 'P30', type: 'line', stack: 'total', areaStyle: { color: '#95de64' }, lineStyle: { color: '#95de64' }, itemStyle: { color: '#95de64' }, emphasis: { focus: 'series' }, data: quarterData.map(d => d.levelCounts.P30), yAxisIndex: 0 },
                    { name: 'P50', type: 'line', stack: 'total', areaStyle: { color: '#ffa940' }, lineStyle: { color: '#ffa940' }, itemStyle: { color: '#ffa940' }, emphasis: { focus: 'series' }, data: quarterData.map(d => d.levelCounts.P50), yAxisIndex: 0 },
                    { name: 'P90', type: 'line', stack: 'total', areaStyle: { color: '#ff4d4f' }, lineStyle: { color: '#ff4d4f' }, itemStyle: { color: '#ff4d4f' }, emphasis: { focus: 'series' }, data: quarterData.map(d => d.levelCounts.P90), yAxisIndex: 0 },
                    { name: 'P10+P30占比', type: 'line', yAxisIndex: 1, data: quarterData.map(d => parseFloat(d.p10p30Ratio)), lineStyle: { type: 'dashed', width: 2, color: '#0a7871' }, symbol: 'circle', symbolSize: 5, itemStyle: { color: '#fff', borderColor: '#0a7871', borderWidth: 1, borderType: 'solid', opacity: 1 }, emphasis: { itemStyle: { borderColor: '#0a7871', borderWidth: 3 } } }
                ]
            };

            chart.setOption(option);
        };

        return (
            <div>
                {/* 季度对比表格 */}
                <Card title="季度等级分布对比" style={{ marginBottom: 24 }}>
                    <Table
                        dataSource={[
                            ...['P10', 'P30', 'P50', 'P90'].map(level => {
                                const prevCount = quarterComparison.previous?.[level] || 0;
                                const currCount = quarterComparison.current?.[level] || 0;
                                const prevTotal = quarterComparison.previous?.total || 0;
                                const currTotal = quarterComparison.current?.total || 0;
                                const prevPercent = prevTotal > 0 ? (prevCount / prevTotal * 100).toFixed(1) : 0;
                                const currPercent = currTotal > 0 ? (currCount / currTotal * 100).toFixed(1) : 0;

                                // 计算累计数据
                                const cumCount = cumulativeStats[level] || 0;
                                const cumTotal = cumulativeStats.total || 0;
                                const cumPercent = cumTotal > 0 ? (cumCount / cumTotal * 100).toFixed(1) : 0;

                                return {
                                    key: level,
                                    level,
                                    previous: prevCount,
                                    previousPercent: prevPercent,
                                    current: currCount,
                                    currentPercent: currPercent,
                                    change: currCount - prevCount,
                                    percentChange: (parseFloat(currPercent) - parseFloat(prevPercent)).toFixed(1),
                                    // 新增累计数据字段
                                    cumulative: `${cumCount} (${cumPercent}%)`
                                };
                            }),
                            // 添加总计行
                            {
                                key: 'total',
                                level: '总计',
                                previous: quarterComparison.previous?.total || 0,
                                previousPercent: '100.0',
                                current: quarterComparison.current?.total || 0,
                                currentPercent: '100.0',
                                change: (quarterComparison.current?.total || 0) - (quarterComparison.previous?.total || 0),
                                percentChange: '0.0',
                                cumulative: `${cumulativeStats.total} (100.0%)`
                            }
                        ]}
                        columns={[
                            {
                                title: '等级',
                                dataIndex: 'level',
                                render: (level) => level === '总计' ?
                                    <strong>{level}</strong> :
                                    <Tag className={`level-tag level-tag-${level}`}>{level}</Tag>
                            },
                            {
                                title: `上季度 (${quarterInfo.previous.label})`,
                                dataIndex: 'previous',
                                render: (val, record) => record.level === '总计' ?
                                    <strong>{val}</strong> : val
                            },
                            {
                                title: `当季度 (${quarterInfo.current.label})`,
                                dataIndex: 'current',
                                render: (val, record) => record.level === '总计' ?
                                    <strong>{val}</strong> : val
                            },
                            {
                                title: '数量变化',
                                dataIndex: 'change',
                                render: (val, record) => {
                                    const style = { color: val > 0 ? '#52c41a' : val < 0 ? '#ff4d4f' : '#666' };
                                    const text = `${val > 0 ? '+' : ''}${val}`;
                                    return record.level === '总计' ?
                                        <strong style={style}>{text}</strong> :
                                        <span style={style}>{text}</span>;
                                }
                            },
                            {
                                title: '占比变化',
                                dataIndex: 'percentChange',
                                render: (val, record) => {
                                    if (record.level === '总计') return '-';

                                    const numVal = parseFloat(val);
                                    const prevPercent = record.previousPercent;
                                    const currPercent = record.currentPercent;
                                    const arrow = numVal > 0 ? '↑' : numVal < 0 ? '↓' : '→';
                                    const color = numVal > 0 ? '#52c41a' : numVal < 0 ? '#ff4d4f' : '#666';

                                    return (
                                        <span style={{ color }}>
                                            {prevPercent}% → {currPercent}% ({arrow}{Math.abs(numVal)}%)
                                        </span>
                                    );
                                }
                            },
                            // 新增：累计上线财务等级列
                            {
                                title: '累计上线财务等级',
                                dataIndex: 'cumulative',
                                render: (val, record) => record.level === '总计' ? <strong>{val}</strong> : val
                            }
                        ]}
                        pagination={false}
                        bordered
                    />
                </Card>

                {/* 趋势图表 */}
                <Card title="财务模型趋势分析" style={{ marginBottom: 24 }}>
                    <div ref={trendChartRef} className="chart-container"></div>
                </Card>

                {/* 大区对比图表 - 增加切换按钮 */}
                <Card
                    title="大区财务等级对比"
                    extra={
                        <Space>
                            <span>数据源:</span>
                            <Segmented
                                options={[
                                    { label: '最新季度', value: false },
                                    { label: '累计数据', value: true }
                                ]}
                                value={useCumulative}
                                onChange={setUseCumulative}
                            />
                        </Space>
                    }
                    style={{ marginBottom: 24 }}
                >
                    <div ref={regionChartRef} className="chart-container"></div>
                </Card>

                {/* 省份对比图表 - 增加切换按钮 */}
                <Card
                    title="省份财务等级排名"
                    extra={
                        <Space>
                            <span>数据源:</span>
                            <Segmented
                                options={[
                                    { label: '最新季度', value: false },
                                    { label: '累计数据', value: true }
                                ]}
                                value={useCumulative}
                                onChange={setUseCumulative}
                            />
                        </Space>
                    }
                    style={{ marginBottom: 24 }}
                >
                    <div ref={provinceChartRef} style={{ height: 800 }}></div>
                </Card>
            </div>
        );
    };

    // 详细分析组件
    const AnalysisView = ({ quarterInfo, rawData, QUARTERS }) => {
        // ===== 1. 状态定义 =====
        const [analysisView, setAnalysisView] = useState('city');
        const [filteredData, setFilteredData] = useState([]);
        const [filterCollapsed, setFilterCollapsed] = useState(true);
        const [searchValue, setSearchValue] = useState('');
        const [searchOptions, setSearchOptions] = useState([]);
        const [activeTag, setActiveTag] = useState('');
        const [firstFilteredData, setFirstFilteredData] = useState([]);
        const [helpModalVisible, setHelpModalVisible] = useState(false);

        const [filters, setFilters] = useState({
            projectNames: [],
            regions: [],
            provinces: [],
            cities: [],
            projectTypes: [],
            productTypes: [],
            currentLevels: [],
            cumulativeLevels: [],
            meetingLevels: []
        });

        const [statistics, setStatistics] = useState({
            totalFiltered: 0,
            excellentStations: 0,
            needAttention: 0,
            p90Stations: 0,
            quarterlyImproved: 0,
            quarterlyDeclined: 0,
            quarterlyStable: 0,
            continuousImproved: 0,
            continuousDeclined: 0,
            stableExcellent: 0,
            needImprovement: 0
        });

        // ===== 2. 函数定义 =====

        // getFilterOptions 函数
        const getFilterOptions = () => {
            const regions = [...new Set(rawData.map(item => item['大区']).filter(Boolean))];
            const provinces = [...new Set(rawData.map(item => item['省份']).filter(Boolean))];
            const cities = [...new Set(rawData.map(item => item['城市']).filter(Boolean))];
            const projectTypes = [...new Set(rawData.map(item => item['项目类型']).filter(Boolean))];
            const productTypes = [...new Set(rawData.map(item => item['场站类型（2C/4C/5C）']).filter(Boolean))];
            const levels = ['P10', 'P30', 'P50', 'P90'];

            return {
                regions,
                provinces,
                cities,
                projectTypes,
                productTypes,
                levels
            };
        };

        // 获取筛选选项
        const filterOptions = getFilterOptions();

        // handleSearch 函数
        const handleSearch = (value) => {
            setSearchValue(value);
            if (value) {
                const options = rawData
                    .filter(item => item['项目名称'] && item['项目名称'].includes(value))
                    .map(item => ({
                        value: item['项目名称'],
                        label: (
                            <span>
                                {item['项目名称'].split(value).map((part, index) => (
                                    index === 0 ? part : <span key={index}><mark style={{ backgroundColor: '#ffc069', padding: 0 }}>{value}</mark>{part}</span>
                                ))}
                            </span>
                        )
                    }))
                    .slice(0, 10);
                setSearchOptions(options);
            } else {
                setSearchOptions([]);
            }
        };

        // calculateStatistics 函数
        const calculateStatistics = (data) => {
            const stats = {
                totalFiltered: data.length,
                excellentStations: 0,
                needAttention: 0,
                p90Stations: 0,
                quarterlyImproved: 0,
                quarterlyDeclined: 0,
                quarterlyStable: 0,
                continuousImproved: 0,
                continuousDeclined: 0,
                stableExcellent: 0,
                needImprovement: 0
            };

            // 计算各种趋势的数量
            data.forEach(item => {
                const trend = getStationTrend(item);

                // 统计各种趋势
                switch (trend) {
                    case 'stableExcellent':
                        stats.stableExcellent++;
                        break;
                    case 'needImprovement':
                        stats.needImprovement++;
                        break;
                    case 'continuousImproved':
                        stats.continuousImproved++;
                        break;
                    case 'continuousDeclined':
                        stats.continuousDeclined++;
                        break;
                    case 'quarterlyImproved':
                        stats.quarterlyImproved++;
                        break;
                    case 'quarterlyDeclined':
                        stats.quarterlyDeclined++;
                        break;
                    case 'quarterlyStable':
                        stats.quarterlyStable++;
                        break;
                }

                // 统计当前季度等级
                const currentLevel = item[quarterInfo.current.field];
                if (currentLevel) {
                    if (currentLevel === 'P10' || currentLevel === 'P30') {
                        stats.excellentStations++;
                    }
                    if (currentLevel === 'P50' || currentLevel === 'P90') {
                        stats.needAttention++;
                    }
                    if (currentLevel === 'P90') {
                        stats.p90Stations++;
                    }
                }
            });

            setStatistics(stats);
        };

        // getStationTrend 函数
        const getStationTrend = (item) => {

            // 添加 QUARTERS 检查
            if (!QUARTERS || QUARTERS.length === 0) {
                return null;
            }
            const currentLevel = item[quarterInfo.current.field];
            const prevLevel = item[quarterInfo.previous.field];

            if (!currentLevel) return null;

            let prevPrevQuarter = null;
            const currentQuarterIndex = QUARTERS.findIndex(q => q.field === quarterInfo.current.field);
            if (currentQuarterIndex >= 2) {
                prevPrevQuarter = QUARTERS[currentQuarterIndex - 2];
            }
            const prevPrevLevel = prevPrevQuarter ? item[prevPrevQuarter.field] : null;

            const levelValue = { 'P10': 4, 'P30': 3, 'P50': 2, 'P90': 1 };

            if ((currentLevel === 'P10' || currentLevel === 'P30') &&
                (prevLevel === 'P10' || prevLevel === 'P30')) {
                return 'stableExcellent';
            }

            if ((currentLevel === 'P50' || currentLevel === 'P90') &&
                (prevLevel === 'P50' || prevLevel === 'P90')) {
                return 'needImprovement';
            }

            if (currentLevel && prevLevel && prevPrevLevel) {
                const currentValue = levelValue[currentLevel];
                const prevValue = levelValue[prevLevel];
                const prevPrevValue = levelValue[prevPrevLevel];

                if (prevValue > prevPrevValue && currentValue > prevValue) {
                    return 'continuousImproved';
                }
                if (prevValue < prevPrevValue && currentValue < prevValue) {
                    return 'continuousDeclined';
                }
            }

            if (currentLevel && prevLevel) {
                const currentValue = levelValue[currentLevel];
                const prevValue = levelValue[prevLevel];

                if (currentValue > prevValue) {
                    return 'quarterlyImproved';
                } else if (currentValue < prevValue) {
                    return 'quarterlyDeclined';
                } else {
                    return 'quarterlyStable';
                }
            }

            return null;
        };

        useEffect(() => {
            if (activeTag === '') {
                setFilteredData([...firstFilteredData]);
            } else {
                const filtered = firstFilteredData.filter(item => getStationTrend(item) === activeTag);
                setFilteredData([...filtered]);
            }
        }, [activeTag, firstFilteredData]);

        // handleTagFilter 函数
        const handleTagFilter = (tagType) => {
            setActiveTag(tagType);
        };

        // applyFilters 函数
        const applyFilters = () => {
            let filtered = [...rawData];

            // 应用各种筛选条件
            if (filters.projectNames.length > 0) {
                filtered = filtered.filter(item =>
                    filters.projectNames.includes(item['项目名称'])
                );
            }

            if (filters.regions.length > 0) {
                filtered = filtered.filter(item => filters.regions.includes(item['大区']));
            }
            if (filters.provinces.length > 0) {
                filtered = filtered.filter(item => filters.provinces.includes(item['省份']));
            }
            if (filters.cities.length > 0) {
                filtered = filtered.filter(item => filters.cities.includes(item['城市']));
            }
            if (filters.projectTypes.length > 0) {
                filtered = filtered.filter(item => filters.projectTypes.includes(item['项目类型']));
            }
            if (filters.productTypes.length > 0) {
                filtered = filtered.filter(item => filters.productTypes.includes(item['场站类型（2C/4C/5C）']));
            }

            if (filters.currentLevels.length > 0) {
                filtered = filtered.filter(item =>
                    filters.currentLevels.includes(item[quarterInfo.current.field])
                );
            }
            if (filters.cumulativeLevels.length > 0) {
                filtered = filtered.filter(item =>
                    filters.cumulativeLevels.includes(item['累计-财务模型'])
                );
            }
            if (filters.meetingLevels.length > 0) {
                filtered = filtered.filter(item =>
                    filters.meetingLevels.includes(item['上会-财务模型'])
                );
            }

            // 重置标签筛选状态
            setActiveTag('');

            // 更新数据
            setFirstFilteredData(filtered);
            setFilteredData(filtered);

            // 重新计算统计
            calculateStatistics(filtered);
        };

        // resetFilters 函数
        const resetFilters = () => {
            setFilters({
                projectNames: [],
                regions: [],
                provinces: [],
                cities: [],
                projectTypes: [],
                productTypes: [],
                currentLevels: [],
                cumulativeLevels: [],
                meetingLevels: []
            });
            setSearchValue('');
            setActiveTag('');
            setFirstFilteredData(rawData);
            setFilteredData(rawData);
            calculateStatistics(rawData);
        };

        // ===== 3. 子组件定义 =====

        // CityAnalysis 组件
        const CityAnalysis = ({ data }) => {
            const [key35CitiesRank, setKey35CitiesRank] = useState([]);
            const [topCities, setTopCities] = useState([]);
            const [bottomCities, setBottomCities] = useState([]);

            useEffect(() => {
                calculateCityRankings();
            }, [data]);

            const calculateCityRankings = () => {
                const cityStats = {};

                data.forEach(item => {
                    const city = item['城市'];
                    const province = item['省份'];
                    const level = item[quarterInfo.current.field];
                    const cumulativeLevel = item['累计-财务模型'];

                    if (city) {
                        if (!cityStats[city]) {
                            cityStats[city] = {
                                city,
                                province,
                                total: 0,
                                p10: 0, p30: 0, p50: 0, p90: 0,
                                cumulativeP10: 0, cumulativeP30: 0, cumulativeP50: 0, cumulativeP90: 0
                            };
                        }

                        cityStats[city].total++;
                        if (level === 'P10') cityStats[city].p10++;
                        if (level === 'P30') cityStats[city].p30++;
                        if (level === 'P50') cityStats[city].p50++;
                        if (level === 'P90') cityStats[city].p90++;

                        if (cumulativeLevel === 'P10') cityStats[city].cumulativeP10++;
                        if (cumulativeLevel === 'P30') cityStats[city].cumulativeP30++;
                        if (cumulativeLevel === 'P50') cityStats[city].cumulativeP50++;
                        if (cumulativeLevel === 'P90') cityStats[city].cumulativeP90++;
                    }
                });

                // 2. 计算比率
                Object.values(cityStats).forEach(stat => {
                    // 季度优良率
                    stat.excellentRate = stat.total > 0 ?
                        ((stat.p10 + stat.p30) / stat.total * 100).toFixed(1) : '0.0';
                    // 累计优良率
                    stat.cumulativeExcellentRate = stat.total > 0 ?
                        ((stat.cumulativeP10 + stat.cumulativeP30) / stat.total * 100).toFixed(1) : '0.0';
                    // 累计需关注率
                    stat.cumulativeNeedAttentionRate = stat.total > 0 ?
                        ((stat.cumulativeP50 + stat.cumulativeP90) / stat.total * 100).toFixed(1) : '0.0';
                });

                // 3. 处理重点35城 (修改点：联动筛选器 + 使用累计数据)
                // 获取当前筛选数据中存在的城市集合
                const availableCities = new Set(data.map(item => item['城市']));

                const key35Stats = KEY_35_CITIES
                    .filter(cityName => availableCities.has(cityName)) // 仅显示筛选范围内存在的城市
                    .map(cityName => {
                        const stat = cityStats[cityName];
                        if (stat) {
                            return stat;
                        } else {
                            // 理论上 filter 后不会走到这里，但保留防御性代码
                            return {
                                city: cityName,
                                total: 0,
                                cumulativeExcellentRate: '0.0', // 使用累计
                                cumulativeP10: 0, cumulativeP30: 0
                            };
                        }
                    })
                    // 使用累计优良率排序
                    .sort((a, b) => parseFloat(b.cumulativeExcellentRate) - parseFloat(a.cumulativeExcellentRate));

                setKey35CitiesRank(key35Stats);

                // 4. 处理 Top 10 (使用累计)
                const eligibleCities = Object.values(cityStats)
                    .filter(stat => stat.total >= 10)
                    .sort((a, b) => parseFloat(b.cumulativeExcellentRate) - parseFloat(a.cumulativeExcellentRate));

                setTopCities(eligibleCities.slice(0, 10));

                // 5. 处理 Bottom 10 (使用累计)
                const bottomCitiesList = Object.values(cityStats)
                    .filter(stat => stat.total >= 10)
                    .sort((a, b) => parseFloat(b.cumulativeNeedAttentionRate) - parseFloat(a.cumulativeNeedAttentionRate))
                    .slice(0, 10);

                setBottomCities(bottomCitiesList);
            };

            return (
                <div>
                    {/* 重点35城优良率排名 (累计) */}
                    <Card title="重点35城优良率排名 (累计数据)" style={{ marginBottom: 24 }}>
                        {key35CitiesRank.length === 0 ? (
                            <div style={{ textAlign: 'center', color: '#999', padding: '20px' }}>当前筛选范围内无重点35城数据</div>
                        ) : (
                            <div style={{
                                display: 'grid',
                                gridTemplateColumns: 'repeat(5, 1fr)',
                                // 动态计算行数，避免空白
                                gridTemplateRows: `repeat(${Math.ceil(key35CitiesRank.length / 5)}, auto)`,
                                gridAutoFlow: 'column', // 保持纵向排列优先
                                gap: '8px'
                            }}>
                                {key35CitiesRank.map((city, index) => (
                                    <div key={city.city} style={{
                                        padding: '8px 12px',
                                        background: '#fafafa',
                                        borderRadius: '6px',
                                        display: 'flex',
                                        alignItems: 'center',
                                        justifyContent: 'space-between',
                                        fontSize: '13px'
                                    }}>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                            <span style={{
                                                fontWeight: 600,
                                                color: index < 3 ? '#52c41a' : '#666',
                                                minWidth: '25px'
                                            }}>
                                                #{index + 1}
                                            </span>
                                            <span>{city.city}</span>
                                        </div>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '12px', fontSize: '12px' }}>
                                            <span style={{ color: '#999' }}>{city.total}个</span>
                                            <span style={{
                                                fontWeight: 600,
                                                color: parseFloat(city.cumulativeExcellentRate) >= 60 ? '#52c41a' : '#666',
                                                minWidth: '35px',
                                                textAlign: 'right'
                                            }}>
                                                {city.cumulativeExcellentRate}%
                                            </span>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </Card>

                    <Row gutter={16}>
                        {/* 优良率TOP10城市 */}
                        <Col span={12}>
                            <Card title="优良率（累计）TOP10城市" style={{ height: '100%' }}>
                                <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                                    {topCities.map((city, index) => (
                                        <div key={city.city} style={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            padding: '16px',
                                            background: '#fafafa',
                                            borderRadius: '8px'
                                        }}>
                                            <div style={{
                                                width: '32px',
                                                height: '32px',
                                                borderRadius: '50%',
                                                background: index < 3 ?
                                                    `rgba(82, 196, 26, ${1 - index * 0.2})` : '#e8e8e8',
                                                display: 'flex',
                                                alignItems: 'center',
                                                justifyContent: 'center',
                                                fontWeight: 600,
                                                color: index < 3 ? '#fff' : '#666',
                                                marginRight: '16px'
                                            }}>
                                                {index + 1}
                                            </div>
                                            <div style={{ flex: 1 }}>
                                                <div style={{ fontSize: '16px', fontWeight: 600 }}>
                                                    {city.city}
                                                </div>
                                                <div style={{ fontSize: '12px', color: '#999' }}>
                                                    {city.province} · {city.total}个站点
                                                </div>
                                            </div>
                                            <div style={{ textAlign: 'right' }}>
                                                <div style={{
                                                    fontSize: '20px',
                                                    fontWeight: 600,
                                                    color: '#52c41a'
                                                }}>
                                                    {city.cumulativeExcellentRate}%
                                                </div>
                                                <div style={{ fontSize: '12px', color: '#999' }}>
                                                    P10+P30: {city.cumulativeP10 + city.cumulativeP30}
                                                </div>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </Card>
                        </Col>

                        {/* 需关注Bottom10城市 */}
                        <Col span={12}>
                            <Card title="重点关注Bottom10城市 (累计)" style={{ height: '100%' }}>
                                <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                                    {bottomCities.map((city, index) => (
                                        <div key={city.city} style={{
                                            display: 'flex',
                                            alignItems: 'center',
                                            padding: '16px',
                                            background: '#fafafa',
                                            borderRadius: '8px'
                                        }}>
                                            <div style={{
                                                width: '32px',
                                                height: '32px',
                                                borderRadius: '50%',
                                                background: index < 3 ?
                                                    `rgba(255, 77, 79, ${1 - index * 0.2})` : '#e8e8e8',
                                                display: 'flex',
                                                alignItems: 'center',
                                                justifyContent: 'center',
                                                fontWeight: 600,
                                                color: index < 3 ? '#fff' : '#666',
                                                marginRight: '16px'
                                            }}>
                                                {index + 1}
                                            </div>
                                            <div style={{ flex: 1 }}>
                                                <div style={{ fontSize: '16px', fontWeight: 600 }}>
                                                    {city.city}
                                                </div>
                                                <div style={{ fontSize: '12px', color: '#999' }}>
                                                    {city.province} · {city.total}个站点
                                                </div>
                                            </div>
                                            <div style={{ textAlign: 'right' }}>
                                                <div style={{
                                                    fontSize: '20px',
                                                    fontWeight: 600,
                                                    color: '#ff4d4f'
                                                }}>
                                                    {city.cumulativeNeedAttentionRate}%
                                                </div>
                                                <div style={{ fontSize: '12px', color: '#999' }}>
                                                    P50+P90: {city.cumulativeP50 + city.cumulativeP90}
                                                </div>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </Card>
                        </Col>
                    </Row>
                </div>
            );
        };

        // StationAnalysis 组件
        const StationAnalysis = ({ data, getStationTrend, quarterInfo, QUARTERS }) => {
            // 获取等级对比趋势
            const getLevelTrend = (currentLevel, prevLevel) => {
                const levelValue = { 'P10': 4, 'P30': 3, 'P50': 2, 'P90': 1 };
                if (!currentLevel || !prevLevel) return null;

                const currentValue = levelValue[currentLevel];
                const prevValue = levelValue[prevLevel];

                if (currentValue > prevValue) {
                    return { icon: '↗', color: '#52c41a' };
                } else if (currentValue < prevValue) {
                    return { icon: '↘', color: '#ff4d4f' };
                } else {
                    return { icon: '→', color: '#666' };
                }
            };

            // ... (getCumulativeVsMeeting 和 trendConfig 保持不变) ...
            const getCumulativeVsMeeting = (item) => {
                const cumulativeLevel = item['累计-财务模型'];
                const meetingLevel = item['上会-财务模型'] || item['过会-财务模型'];
                if (!cumulativeLevel || !meetingLevel || !item[quarterInfo.current.field]) return '-';
                const levelValue = { 'P10': 4, 'P30': 3, 'P50': 2, 'P90': 1 };
                const diff = levelValue[cumulativeLevel] - levelValue[meetingLevel];
                if (diff > 0) return <span style={{ color: '#52c41a', fontWeight: 500 }}>↑ {Math.abs(diff)}级</span>;
                else if (diff < 0) return <span style={{ color: '#ff4d4f', fontWeight: 500 }}>↓ {Math.abs(diff)}级</span>;
                return <span style={{ color: '#666', fontWeight: 500 }}>→ 持平</span>;
            };

            const trendConfig = {
                'continuousImproved': { label: '连续提升', icon: '😊😊', color: '#52c41a' },
                'continuousDeclined': { label: '连续下降', icon: '😡😡', color: '#ff4d4f' },
                'quarterlyImproved': { label: '季度提升', icon: '😊', color: '#52c41a' },
                'quarterlyDeclined': { label: '季度下降', icon: '😡', color: '#ff4d4f' },
                'quarterlyStable': { label: '保持稳定', icon: '→', color: '#666' },
                'stableExcellent': { label: '稳定优良', icon: '👍', color: '#52c41a' },
                'needImprovement': { label: '待改进', icon: '⚠️', color: '#ff4d4f' }
            };

            const isValidLevel = (val) => {
                if (!val) return false; // null, undefined, ""
                const str = String(val).toUpperCase();
                return str !== 'NAN' && str !== '#N/A' && str !== 'NULL' && str !== '0';
            };

            const columns = [
                {
                    title: '序号',
                    key: 'index',
                    fixed: 'left',
                    width: 60,
                    render: (text, record, index) => index + 1
                },
                {
                    title: '场站名称',
                    dataIndex: '项目名称',
                    key: 'name',
                    fixed: 'left',
                    width: 250,
                    render: (name, record) => (
                        <div>
                            <div style={{ fontWeight: 500 }}>{name}</div>
                            <div style={{ fontSize: '12px', color: '#999' }}>
                                {[
                                    record['大区'],
                                    record['城市'],
                                    record['网络发展人员'],
                                    record['项目类型'],
                                    record['场站类型（2C/4C/5C）']
                                ].filter(Boolean).join(' · ')}
                            </div>
                        </div>
                    )
                },
                {
                    title: '上会等级',
                    dataIndex: '上会-财务模型',
                    key: 'meetingLevel',
                    fixed: 'left',
                    width: 100,
                    render: (level, record) => {
                        const actualLevel = level || record['过会-财务模型'];
                        return actualLevel ? <Tag className={`level-tag level-tag-${actualLevel}`}>{actualLevel}</Tag> : '-';
                    }
                },
                ...(QUARTERS && QUARTERS.length > 0 ? QUARTERS.map((q, index) => ({
                    title: q.label,
                    dataIndex: q.field,
                    key: q.key,
                    width: 100,
                    render: (level, record) => {
                        // 1. 严格判断当前值是否有效
                        if (!isValidLevel(level)) return '-';

                        let prevLevel = null;
                        let compareSource = ''; // 用于调试：看看到底对比的是谁

                        // 2. 回溯查找：往前找最近一个“有效”的季度
                        for (let i = index - 1; i >= 0; i--) {
                            const pastQuarterField = QUARTERS[i].field;
                            const pastValue = record[pastQuarterField];

                            // 关键修改：这里也要判断历史数据是否有效，不能只判断是否存在
                            if (isValidLevel(pastValue)) {
                                prevLevel = pastValue;
                                compareSource = QUARTERS[i].label;
                                break;
                            }
                        }

                        // 3. 兜底逻辑：如果前面全是无效数据，对比上会
                        if (!prevLevel) {
                            const meeting = record['上会-财务模型'] || record['过会-财务模型'];
                            if (isValidLevel(meeting)) {
                                prevLevel = meeting;
                                compareSource = '上会';
                            }
                        }

                        // 4. 调试日志（如果你在控制台F12能看到这个，说明代码生效了）
                        // console.log(`项目:${record['项目名称']}, 季度:${q.label}, 当前:${level}, 对比对象:${compareSource}, 对比值:${prevLevel}`);

                        const trend = getLevelTrend(level, prevLevel);

                        return (
                            <Space size={4}>
                                <Tag className={`level-tag level-tag-${level}`}>{level}</Tag>
                                {trend && (
                                    <span style={{
                                        color: trend.color,
                                        fontWeight: 'bold',
                                        fontSize: '16px'
                                    }}>{trend.icon}</span>
                                )}
                            </Space>
                        );
                    }
                })) : []),
                {
                    title: '累计等级',
                    dataIndex: '累计-财务模型',
                    key: 'cumulativeLevel',
                    width: 100,
                    render: level => level ? <Tag className={`level-tag level-tag-${level}`}>{level}</Tag> : '-'
                },
                {
                    title: '趋势',
                    key: 'trend',
                    width: 120,
                    render: (_, record) => {
                        const trend = getStationTrend(record);
                        if (!trend || !record[quarterInfo.current.field]) return '-';
                        const config = trendConfig[trend];
                        return (
                            <Tag
                                color="default"
                                style={{
                                    background: 'transparent',
                                    border: `1px solid ${config.color}`,
                                    color: config.color
                                }}
                            >
                                <Space size={4}>
                                    <span>{config.icon}</span>
                                    <span>{config.label}</span>
                                </Space>
                            </Tag>
                        );
                    }
                },
                {
                    title: '累计vs上会',
                    key: 'cumulativeVsMeeting',
                    width: 120,
                    render: (_, record) => getCumulativeVsMeeting(record)
                }
            ];

            return (
                <Card>
                    <Table
                        dataSource={data}
                        columns={columns}
                        rowKey={(record, index) => index}
                        scroll={{ x: 1500, y: 600 }}
                        sticky
                        pagination={{
                            defaultPageSize: 20,
                            pageSizeOptions: ['20', '50', '100', '200'],
                            showSizeChanger: true,
                            showQuickJumper: true,
                            showTotal: (total) => `共 ${total} 条`
                        }}
                    />
                </Card>
            );
        };

        // ===== 4. useEffect =====
        useEffect(() => {
            if (rawData && rawData.length > 0) {
                setFirstFilteredData(rawData);
                setFilteredData(rawData);
                calculateStatistics(rawData);
            }
        }, [rawData]);

        // ===== 5. 返回 JSX =====
        return (
            <div>
                {/* 修改筛选区域 - 问题11：折叠时保留搜索框 */}
                <Card
                    style={{ marginBottom: 16 }}
                    bodyStyle={{ padding: filterCollapsed ? '12px 24px' : '24px' }}
                >
                    <div style={{ marginBottom: filterCollapsed ? 0 : 16 }}>
                        {filterCollapsed ? (
                            // 折叠状态显示搜索框和按钮
                            <Row gutter={16} align="middle">
                                <Col flex="1">
                                    <Select
                                        showSearch
                                        value={filters.projectNames}
                                        placeholder="请输入项目名称"
                                        style={{ width: '100%' }}
                                        filterOption={false}
                                        onSearch={handleSearch}
                                        options={searchOptions}
                                        onChange={(value) => setFilters({ ...filters, projectNames: value })}
                                        mode="multiple"
                                    />
                                </Col>
                                <Col>
                                    <Space>
                                        <Button onClick={applyFilters} type="primary">查询</Button>
                                        <Button onClick={resetFilters}>重置</Button>
                                        <Button
                                            type="text"
                                            icon={<icons.DownOutlined />}
                                            onClick={() => setFilterCollapsed(!filterCollapsed)}
                                        >
                                            更多筛选
                                        </Button>
                                    </Space>
                                </Col>
                            </Row>
                        ) : (
                            // 展开状态 - 完整的筛选项
                            <div>
                                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end' }}>
                                    <Button
                                        type="text"
                                        icon={<icons.UpOutlined />}
                                        onClick={() => setFilterCollapsed(!filterCollapsed)}
                                    >
                                        收起
                                    </Button>
                                </div>

                                <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
                                    <Col span={24}>
                                        <Space direction="vertical" style={{ width: '100%' }}>
                                            <Text strong>项目名称</Text>
                                            <Select
                                                showSearch
                                                value={filters.projectNames}
                                                placeholder="请输入项目名称"
                                                style={{ width: '100%' }}
                                                filterOption={false}
                                                onSearch={handleSearch}
                                                options={searchOptions}
                                                onChange={(value) => setFilters({ ...filters, projectNames: value })}
                                                mode="multiple"
                                            />
                                        </Space>
                                    </Col>
                                </Row>

                                <Divider style={{ margin: '16px 0' }}>场站维度筛选</Divider>

                                <Row gutter={[16, 16]}>
                                    <Col span={8}>
                                        <Space direction="vertical" style={{ width: '100%' }}>
                                            <Text>大区</Text>
                                            <Select
                                                mode="multiple"
                                                placeholder="请选择大区"
                                                style={{ width: '100%' }}
                                                value={filters.regions}
                                                onChange={(value) => setFilters({ ...filters, regions: value })}
                                                options={filterOptions.regions.map(r => ({ label: r, value: r }))}
                                            />
                                        </Space>
                                    </Col>
                                    <Col span={8}>
                                        <Space direction="vertical" style={{ width: '100%' }}>
                                            <Text>省份</Text>
                                            <Select
                                                mode="multiple"
                                                placeholder="请选择省份"
                                                style={{ width: '100%' }}
                                                value={filters.provinces}
                                                onChange={(value) => setFilters({ ...filters, provinces: value })}
                                                options={filterOptions.provinces.map(p => ({ label: p, value: p }))}
                                            />
                                        </Space>
                                    </Col>
                                    <Col span={8}>
                                        <Space direction="vertical" style={{ width: '100%' }}>
                                            <Text>城市</Text>
                                            <Select
                                                mode="multiple"
                                                placeholder="请选择城市"
                                                style={{ width: '100%' }}
                                                value={filters.cities}
                                                onChange={(value) => setFilters({ ...filters, cities: value })}
                                                options={filterOptions.cities.map(c => ({ label: c, value: c }))}
                                            />
                                        </Space>
                                    </Col>
                                    <Col span={8}>
                                        <Space direction="vertical" style={{ width: '100%' }}>
                                            <Text>项目类型</Text>
                                            <Select
                                                mode="multiple"
                                                placeholder="请选择项目类型"
                                                style={{ width: '100%' }}
                                                value={filters.projectTypes}
                                                onChange={(value) => setFilters({ ...filters, projectTypes: value })}
                                                options={filterOptions.projectTypes.map(t => ({ label: t, value: t }))}
                                            />
                                        </Space>
                                    </Col>
                                    <Col span={8}>
                                        <Space direction="vertical" style={{ width: '100%' }}>
                                            <Text>产品类型</Text>
                                            <Select
                                                mode="multiple"
                                                placeholder="请选择产品类型"
                                                style={{ width: '100%' }}
                                                value={filters.productTypes}
                                                onChange={(value) => setFilters({ ...filters, productTypes: value })}
                                                options={filterOptions.productTypes.map(t => ({ label: t, value: t }))}
                                            />
                                        </Space>
                                    </Col>
                                </Row>

                                <Divider style={{ margin: '16px 0' }}>财务维度筛选</Divider>

                                <Row gutter={[16, 16]}>
                                    <Col span={8}>
                                        <Space direction="vertical" style={{ width: '100%' }}>
                                            <Text>本季度等级</Text>
                                            <Select
                                                mode="multiple"
                                                placeholder="请选择等级"
                                                style={{ width: '100%' }}
                                                value={filters.currentLevels}
                                                onChange={(value) => setFilters({ ...filters, currentLevels: value })}
                                                options={filterOptions.levels.map(l => ({
                                                    label: <Tag className={`level-tag level-tag-${l}`}>{l}</Tag>,
                                                    value: l
                                                }))}
                                            />
                                        </Space>
                                    </Col>
                                    <Col span={8}>
                                        <Space direction="vertical" style={{ width: '100%' }}>
                                            <Text>累计等级</Text>
                                            <Select
                                                mode="multiple"
                                                placeholder="请选择等级"
                                                style={{ width: '100%' }}
                                                value={filters.cumulativeLevels}
                                                onChange={(value) => setFilters({ ...filters, cumulativeLevels: value })}
                                                options={filterOptions.levels.map(l => ({
                                                    label: <Tag className={`level-tag level-tag-${l}`}>{l}</Tag>,
                                                    value: l
                                                }))}
                                            />
                                        </Space>
                                    </Col>
                                    <Col span={8}>
                                        <Space direction="vertical" style={{ width: '100%' }}>
                                            <Text>上会等级</Text>
                                            <Select
                                                mode="multiple"
                                                placeholder="请选择等级"
                                                style={{ width: '100%' }}
                                                value={filters.meetingLevels}
                                                onChange={(value) => setFilters({ ...filters, meetingLevels: value })}
                                                options={filterOptions.levels.map(l => ({
                                                    label: <Tag className={`level-tag level-tag-${l}`}>{l}</Tag>,
                                                    value: l
                                                }))}
                                            />
                                        </Space>
                                    </Col>
                                </Row>

                                <Row style={{ marginTop: 24 }}>
                                    <Col span={24} style={{ textAlign: 'right' }}>
                                        <Space>
                                            <Button onClick={applyFilters} type="primary">查询</Button>
                                            <Button onClick={resetFilters}>重置</Button>
                                        </Space>
                                    </Col>
                                </Row>
                            </div>
                        )}
                    </div>
                </Card>

                {/* 修改统计指标行 - 问题9、10 */}
                <Card style={{ marginBottom: 16 }} bodyStyle={{ padding: '12px 24px' }}>
                    {/* 第一行：统计数据 */}
                    <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'flex-end',
                        gap: '24px',
                        marginBottom: '12px'
                    }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                            <icons.CheckCircleFilled style={{ color: '#52c41a', fontSize: '16px' }} />
                            <span style={{ fontSize: '13px', color: '#666' }}>优良站点</span>
                            <span style={{ fontSize: '16px', fontWeight: 'bold' }}>{statistics.excellentStations}</span>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                            <icons.WarningFilled style={{ color: '#faad14', fontSize: '16px' }} />
                            <span style={{ fontSize: '13px', color: '#666' }}>需关注</span>
                            <span style={{ fontSize: '16px', fontWeight: 'bold' }}>{statistics.needAttention}</span>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                            <icons.CloseCircleFilled style={{ color: '#ff4d4f', fontSize: '16px' }} />
                            <span style={{ fontSize: '13px', color: '#666' }}>P90项目</span>
                            <span style={{ fontSize: '16px', fontWeight: 'bold' }}>{statistics.p90Stations}</span>
                        </div>

                        {/* 分隔线 */}
                        <Divider type="vertical" />

                        {/* 说明按钮 */}
                        <Button
                            type="text"
                            size="small"
                            icon={<icons.QuestionCircleOutlined />}
                            onClick={() => setHelpModalVisible(true)}
                        >
                            指标含义
                        </Button>

                    </div>

                    {/* 第二行：筛选按钮组 */}
                    <div style={{
                        display: 'flex',
                        gap: '8px',
                        flexWrap: 'wrap'
                    }}>
                        <Button
                            size="small"
                            shape="round"
                            type={activeTag === '' ? 'primary' : 'default'}
                            onClick={() => handleTagFilter('')}
                        >
                            全部 ({statistics.totalFiltered})
                        </Button>

                        <Button
                            size="small"
                            shape="round"
                            type={activeTag === 'stableExcellent' ? 'primary' : 'default'}
                            onClick={() => handleTagFilter('stableExcellent')}
                        >
                            👍 稳定优良 ({statistics.stableExcellent})
                        </Button>

                        <Button
                            size="small"
                            shape="round"
                            type={activeTag === 'needImprovement' ? 'primary' : 'default'}
                            onClick={() => handleTagFilter('needImprovement')}
                        >
                            ⚠️ 待改进 ({statistics.needImprovement})
                        </Button>

                        <Button
                            size="small"
                            shape="round"
                            type={activeTag === 'continuousImproved' ? 'primary' : 'default'}
                            onClick={() => handleTagFilter('continuousImproved')}
                        >
                            😊😊 连续提升 ({statistics.continuousImproved})
                        </Button>

                        <Button
                            size="small"
                            shape="round"
                            type={activeTag === 'continuousDeclined' ? 'primary' : 'default'}
                            onClick={() => handleTagFilter('continuousDeclined')}
                        >
                            😡😡 连续下降 ({statistics.continuousDeclined})
                        </Button>

                        <Button
                            size="small"
                            shape="round"
                            type={activeTag === 'quarterlyImproved' ? 'primary' : 'default'}
                            onClick={() => handleTagFilter('quarterlyImproved')}
                        >
                            😊 季度提升 ({statistics.quarterlyImproved})
                        </Button>

                        <Button
                            size="small"
                            shape="round"
                            type={activeTag === 'quarterlyDeclined' ? 'primary' : 'default'}
                            onClick={() => handleTagFilter('quarterlyDeclined')}
                        >
                            😡 季度下降 ({statistics.quarterlyDeclined})
                        </Button>

                        <Button
                            size="small"
                            shape="round"
                            type={activeTag === 'quarterlyStable' ? 'primary' : 'default'}
                            onClick={() => handleTagFilter('quarterlyStable')}
                        >
                            → 保持稳定 ({statistics.quarterlyStable})
                        </Button>
                    </div>
                </Card>

                {/* 视图切换和内容展示 */}
                <Segmented
                    options={[
                        { label: '城市分析', value: 'city' },
                        { label: '单站分析', value: 'station' }
                    ]}
                    value={analysisView}
                    onChange={setAnalysisView}
                    style={{ marginBottom: 24 }}
                />

                {/* 在组件最后添加模态框 */}
                <Modal
                    title="标签计算逻辑说明"
                    open={helpModalVisible}
                    onCancel={() => setHelpModalVisible(false)}
                    footer={[
                        <Button key="close" onClick={() => setHelpModalVisible(false)}>
                            关闭
                        </Button>
                    ]}
                    width={800}
                >
                    <div style={{ lineHeight: 1.6 }}>
                        <Title level={4}>📊 基础分类</Title>
                        <ul>
                            <li><strong>优良站点：</strong>当前季度等级为P10或P30的站点</li>
                            <li><strong>需关注：</strong>当前季度等级为P50或P90的站点</li>
                            <li><strong>P90项目：</strong>当前季度等级为P90的站点</li>
                        </ul>

                        <Title level={4}>🔄 趋势分类逻辑</Title>

                        <div style={{ marginBottom: 16 }}>
                            <Text strong>👍 稳定优良：</Text>
                            <Text> 当前季度和上季度都是P10或P30</Text>
                        </div>

                        <div style={{ marginBottom: 16 }}>
                            <Text strong>⚠️ 待改进：</Text>
                            <Text> 当前季度和上季度都是P50或P90</Text>
                        </div>

                        <div style={{ marginBottom: 16 }}>
                            <Text strong>😊😊 连续提升：</Text>
                            <Text> 连续两个季度等级逐步提升（例：P90→P50→P30）</Text>
                        </div>

                        <div style={{ marginBottom: 16 }}>
                            <Text strong>😡😡 连续下降：</Text>
                            <Text> 连续两个季度等级逐步下降（例：P30→P50→P90）</Text>
                        </div>

                        <div style={{ marginBottom: 16 }}>
                            <Text strong>😊 季度提升：</Text>
                            <Text> 比上季度等级有所提升</Text>
                        </div>

                        <div style={{ marginBottom: 16 }}>
                            <Text strong>😡 季度下降：</Text>
                            <Text> 比上季度等级有所下降</Text>
                        </div>

                        <div style={{ marginBottom: 16 }}>
                            <Text strong>→ 保持稳定：</Text>
                            <Text> 与上季度等级相同</Text>
                        </div>

                        <Alert
                            message="等级排序"
                            description="P10 > P30 > P50 > P90 （P10为最优等级，P90为最差等级）"
                            type="info"
                            showIcon
                            style={{ marginTop: 16 }}
                        />
                    </div>
                </Modal>

                {analysisView === 'city' ? (
                    <CityAnalysis data={filteredData} />
                ) : (
                    <StationAnalysis
                        data={filteredData}
                        getStationTrend={getStationTrend}
                        quarterInfo={quarterInfo}
                        QUARTERS={QUARTERS}  // 添加 QUARTERS
                    />
                )}
            </div>
        );
    };

    // 汇总分析子组件
    const SummaryAnalysis = ({ data }) => {
        const [tagStats, setTagStats] = useState([]);

        useEffect(() => {
            calculateTagStatistics();
        }, [data]);

        const calculateTagStatistics = () => {
            // 计算各种标签统计...
            const stats = [
                { key: 'improved', label: '季度提升', count: 0, icon: <RiseOutlined /> },
                { key: 'declined', label: '季度下降', count: 0, icon: <FallOutlined /> },
                { key: 'stable', label: '保持稳定', count: 0, icon: <MinusOutlined /> }
            ];
            setTagStats(stats);
        };

        return (
            <Row gutter={[16, 16]}>
                <Col span={24}>
                    <div className="stat-grid">
                        {tagStats.map(stat => (
                            <Card key={stat.key} hoverable>
                                <Statistic
                                    title={stat.label}
                                    value={stat.count}
                                    prefix={stat.icon}
                                />
                            </Card>
                        ))}
                    </div>
                </Col>
            </Row>
        );
    };

    return (
        <Layout className="app-layout">
            <Header className="app-header">
                <div className="app-logo">
                    <DashboardOutlined style={{ fontSize: 24, color: '#0a7871' }} />
                    <span>城市站评价模型财务结果追踪看板</span>
                    <Badge
                        count="试用版"
                        style={{ backgroundColor: '#d0a675' }}
                    />
                    {quarterInfo.current.label && (
                        <Tag
                            color="#0a7871"
                            icon={<EnvironmentOutlined />}
                        >
                            {quarterInfo.current.label}
                        </Tag>
                    )}
                    {/* 修改总场站数显示 */}
                    {quarterInfo.current.field && rawData.length > 0 && (
                        <Tag icon={<BankOutlined />}>
                            总场站数: {(() => {
                                const count = rawData.filter(item =>
                                    item[quarterInfo.current.field]
                                ).length;
                                return count.toLocaleString();
                            })()}
                        </Tag>
                    )}
                </div>
            </Header>

            <Content className="app-content">


                <Spin
                    spinning={dataLoading || !quartersLoaded}
                    tip={!quartersLoaded ? "加载季度配置中..." : "数据加载中..."}
                >
                    {invalidDataCount > 0 && !alertDismissed && (
                        <Alert
                            message={`发现 ${invalidDataCount} 条无效数据（#N/A），已自动过滤`}
                            type="warning"
                            closable
                            onClose={() => setAlertDismissed(true)}
                            style={{ marginBottom: 16 }}
                        />
                    )}

                    {/* 只有在季度配置加载完成后才显示 Tabs */}
                    {quartersLoaded && QUARTERS.length > 0 ? (
                        <Tabs
                            activeKey={currentPage}
                            onChange={setCurrentPage}
                            items={[
                                {
                                    key: 'guide',
                                    label: '指标指南',
                                    children: <GuideView />
                                },
                                {
                                    key: 'dashboard',
                                    label: '概览看板',
                                    children: <DashboardView currentPage={currentPage} QUARTERS={QUARTERS} />
                                },
                                {
                                    key: 'analysis',
                                    label: '详细分析',
                                    children: (
                                        <AnalysisView
                                            quarterInfo={quarterInfo}
                                            rawData={rawData}
                                            QUARTERS={QUARTERS}  // 添加 QUARTERS
                                        />
                                    )
                                }
                            ]}
                        />
                    ) : (
                        <div style={{ textAlign: 'center', padding: '50px' }}>
                            <Spin size="large" />
                            <p style={{ marginTop: 16 }}>正在初始化系统...</p>
                        </div>
                    )}
                </Spin>
            </Content>
        </Layout>
    );
};

// 渲染应用
ReactDOM.render(<App />, document.getElementById('root'));