import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime

# 设置页面配置
st.set_page_config(
    page_title="财务模型监控看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS样式
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    div[data-testid="metric-container"] {
        background-color: #f0f2f6;
        border: 2px solid #cccccc;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .uploadfile {
        text-align: center;
        padding: 20px;
        border: 2px dashed #cccccc;
        border-radius: 10px;
        margin: 20px 0;
    }
    </style>
""", unsafe_allow_html=True)

# 初始化session state
if 'data' not in st.session_state:
    st.session_state.data = None

# 定义季度列表（根据您的实际列名）
quarters = ['2024年一季度财务模型', '2024年二季度财务模型', '2024年三季度财务模型', 
           '2024年四季度财务模型', '2025年一季度财务模型', '2025年二季度财务模型', 
           '2025年三季度财务模型']

# 季度显示名称映射
quarter_display_names = {
    '2024年一季度财务模型': '2024Q1',
    '2024年二季度财务模型': '2024Q2',
    '2024年三季度财务模型': '2024Q3',
    '2024年四季度财务模型': '2024Q4',
    '2025年一季度财务模型': '2025Q1',
    '2025年二季度财务模型': '2025Q2',
    '2025年三季度财务模型': '2025Q3'
}

# 定义颜色映射
color_map = {
    'P10': '#2E7D32',  # 深绿色
    'P30': '#66BB6A',  # 浅绿色
    'P50': '#FFA726',  # 橘黄色
    'P90': '#EF5350'   # 红色
}

# 侧边栏导航
st.sidebar.title("📊 财务模型监控看板")
page = st.sidebar.radio(
    "请选择页面",
    ["📖 使用指南", "📈 概览看板", "🔍 详细分析", "📤 数据上传"]
)

# 数据上传页面
if page == "📤 数据上传":
    st.title("📤 数据上传")
    st.markdown("### 请上传Excel数据文件")
    
    uploaded_file = st.file_uploader(
        "选择Excel文件（.xlsx或.xls）",
        type=['xlsx', 'xls'],
        help="请确保数据包含所有必需的字段"
    )
    
    if uploaded_file is not None:
        try:
            df = pd.read_excel(uploaded_file)
            st.session_state.data = df
            
            st.success("✅ 数据上传成功！")
            st.markdown("### 数据预览")
            st.dataframe(df.head(10))
            
            st.markdown("### 数据统计")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("总记录数", len(df))
            with col2:
                st.metric("大区数量", df['大区'].nunique())
            with col3:
                st.metric("城市数量", df['城市'].nunique())
            with col4:
                st.metric("项目类型数", df['项目类型'].nunique())
            
            # 显示列名以便调试
            with st.expander("查看数据列名"):
                st.write(df.columns.tolist())
                
        except Exception as e:
            st.error(f"❌ 数据读取失败：{str(e)}")

# 使用指南页面
elif page == "📖 使用指南":
    st.title("📖 财务模型看板使用指南")
    
    tab1, tab2, tab3 = st.tabs(["📊 上会财务模型计算逻辑", "📈 上线季度财务结果", "📝 指标说明"])
    
    with tab1:
        st.markdown("""
        ### 上会财务模型计算逻辑
        
        **1. 评级标准**
        - **P10级**：财务表现优秀，投资回报率高，风险低
        - **P30级**：财务表现良好，投资回报率较高，风险可控
        - **P50级**：财务表现一般，投资回报率中等，需要关注风险
        - **P90级**：财务表现较差，投资回报率低，风险较高
        
        **2. 评定流程**
        - 项目上会时，根据财务模型测算结果进行初步评级
        - 综合考虑市场潜力、成本控制、收益预期等多个维度
        - 由财务委员会最终确定项目的财务等级
        
        **3. 动态调整机制**
        - 每季度根据实际运营数据重新评估
        - 对比预测模型与实际表现的差异
        - 必要时调整财务等级
        """)
    
    with tab2:
        st.markdown("""
        ### 上线季度财务结果
        
        **1. 季度评估周期**
        - 每季度末进行财务结果评估
        - 基于实际运营数据计算关键财务指标
        - 对比上会时的预测模型进行差异分析
        
        **2. 评估维度**
        - **收入达成率**：实际收入/预测收入
        - **成本控制率**：实际成本/预算成本
        - **利润率**：实际利润率vs预测利润率
        - **投资回收期**：实际回收期vs预测回收期
        
        **3. 等级调整规则**
        - 表现超预期20%以上：考虑上调等级
        - 表现低于预期20%以上：考虑下调等级
        - 连续两个季度表现稳定：维持当前等级
        """)
    
    with tab3:
        st.markdown("""
        ### 指标说明
        
        **1. 优良率**
        - 定义：(P10数量 + P30数量) / 总站点数量 × 100%
        - 意义：反映整体项目质量水平
        - 目标：优良率 > 60%
        
        **2. 需关注率**
        - 定义：(P50数量 + P90数量) / 总站点数量 × 100%
        - 意义：识别需要重点改进的项目
        - 目标：需关注率 < 40%
        
        **3. 趋势分析**
        - **提升**：本季度等级优于上季度
        - **下降**：本季度等级劣于上季度
        - **稳定**：连续两季度等级不变
        - **连续提升**：连续两季度等级提升
        - **连续下降**：连续两季度等级下降
        
        **4. 大区分类**
        - 东一区、东二区：东部沿海发达地区
        - 南区：南部经济活跃区域
        - 北区：北部重点城市群
        - 西区：西部战略发展区域
        - 中区：中部核心城市带
        """)

# 概览看板页面
elif page == "📈 概览看板":
    st.title("📈 概览看板")
    
    if st.session_state.data is None:
        st.warning("⚠️ 请先上传数据文件")
        st.stop()
    
    df = st.session_state.data
    
    # 获取当前季度（最新有数据的季度）
    current_quarter = '2025年三季度财务模型'
    prev_quarter = '2025年二季度财务模型'
    
    # 计算总站点数
    total_sites = len(df)
    
    # 计算有效数据（排除空值）
    current_data = df[df[current_quarter].notna()] if current_quarter in df.columns else pd.DataFrame()
    prev_data = df[df[prev_quarter].notna()] if prev_quarter in df.columns else pd.DataFrame()
    
    # 计算增长率
    if not prev_data.empty and not current_data.empty:
        growth_count = len(current_data) - len(prev_data)
        growth_rate = (growth_count / len(prev_data) * 100) if len(prev_data) > 0 else 0
    else:
        growth_rate = 0
    
    # 顶部卡片
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="总场站数",
            value=total_sites,
            delta=f"{growth_rate:.1f}%"
        )
    with col2:
        # 计算当前季度有数据的站点数
        active_sites = len(current_data) if not current_data.empty else 0
        st.metric(
            label="活跃站点数",
            value=active_sites,
            delta="本季度有数据"
        )
    with col3:
        # 计算优良率
        if current_quarter in df.columns:
            valid_data = df[df[current_quarter].notna()]
            if not valid_data.empty:
                counts = valid_data[current_quarter].value_counts()
                excellent = counts.get('P10', 0) + counts.get('P30', 0)
                total_valid = counts.sum()
                excellent_rate = (excellent / total_valid * 100) if total_valid > 0 else 0
            else:
                excellent_rate = 0
        else:
            excellent_rate = 0
        
        st.metric(
            label="优良率",
            value=f"{excellent_rate:.1f}%",
            delta="P10+P30占比"
        )
    with col4:
        # 计算需关注率
        if current_quarter in df.columns:
            valid_data = df[df[current_quarter].notna()]
            if not valid_data.empty:
                counts = valid_data[current_quarter].value_counts()
                concern = counts.get('P50', 0) + counts.get('P90', 0)
                total_valid = counts.sum()
                concern_rate = (concern / total_valid * 100) if total_valid > 0 else 0
            else:
                concern_rate = 0
        else:
            concern_rate = 0
        
        st.metric(
            label="需关注率",
            value=f"{concern_rate:.1f}%",
            delta="P50+P90占比"
        )
    
    st.markdown("---")
    
    # 季度财务指标对比表格
    st.markdown("### 📊 季度财务指标对比")
    
    # 统计当前季度和上季度各等级数量
    if current_quarter in df.columns:
        current_counts = df[df[current_quarter].notna()][current_quarter].value_counts()
        total_current = current_counts.sum()
    else:
        current_counts = pd.Series()
        total_current = 0
    
    if prev_quarter in df.columns:
        prev_counts = df[df[prev_quarter].notna()][prev_quarter].value_counts()
        total_prev = prev_counts.sum()
    else:
        prev_counts = pd.Series()
        total_prev = 0
    
    # 创建对比表格数据
    comparison_data = []
    for grade in ['P10', 'P30', 'P50', 'P90']:
        current_count = current_counts.get(grade, 0)
        current_pct = (current_count / total_current * 100) if total_current > 0 else 0
        
        prev_count = prev_counts.get(grade, 0)
        prev_pct = (prev_count / total_prev * 100) if total_prev > 0 else 0
        
        change_count = current_count - prev_count
        change_pct = current_pct - prev_pct
        
        comparison_data.append({
            '财务指标': grade,
            '当季度-数量': current_count,
            '当季度-比例': f"{current_pct:.1f}%",
            '上季度-数量': prev_count,
            '上季度-比例': f"{prev_pct:.1f}%",
            '变化-数量': f"{change_count:+d}",
            '变化-比例': f"{change_pct:+.1f}%"
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    st.dataframe(comparison_df, hide_index=True, use_container_width=True)
    
    # 两列布局
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📈 全国维度各季度财务指标趋势")
        
        # 准备趋势数据
        trend_data = []
        for q in quarters:
            if q in df.columns:
                valid_data = df[df[q].notna()]
                if not valid_data.empty:
                    counts = valid_data[q].value_counts()
                    for grade in ['P10', 'P30', 'P50', 'P90']:
                        trend_data.append({
                            '季度': quarter_display_names.get(q, q),
                            '财务等级': grade,
                            '数量': counts.get(grade, 0)
                        })
        
        if trend_data:
            trend_df = pd.DataFrame(trend_data)
            
            # 创建堆积面积图
            fig = px.area(
                trend_df,
                x='季度',
                y='数量',
                color='财务等级',
                color_discrete_map=color_map,
                title='',
                labels={'数量': '项目数量', '季度': ''},
            )
            
            fig.update_layout(
                height=400,
                hovermode='x unified',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("暂无趋势数据")
    
    with col2:
        st.markdown("### 📊 各大区财务模型分布")
        
        # 准备大区数据
        district_data = []
        if current_quarter in df.columns:
            for district in ['东一区', '东二区', '南区', '北区', '西区', '中区']:
                district_df = df[df['大区'] == district]
                if len(district_df) > 0:
                    valid_data = district_df[district_df[current_quarter].notna()]
                    if not valid_data.empty:
                        counts = valid_data[current_quarter].value_counts()
                        for grade in ['P10', 'P30', 'P50', 'P90']:
                            district_data.append({
                                '大区': district,
                                '财务等级': grade,
                                '数量': counts.get(grade, 0)
                            })
        
        if district_data:
            district_df = pd.DataFrame(district_data)
            
            # 创建堆积柱形图
            fig = px.bar(
                district_df,
                x='大区',
                y='数量',
                color='财务等级',
                color_discrete_map=color_map,
                title='',
                labels={'数量': '项目数量', '大区': ''},
            )
            
            fig.update_layout(
                height=400,
                barmode='stack',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode='x unified'
            )
            
            # 添加数值标签
            fig.update_traces(
                texttemplate='%{y}',
                textposition='inside',
                textfont_size=10
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("暂无大区数据")

# 详细分析页面
elif page == "🔍 详细分析":
    st.title("🔍 详细分析")
    
    if st.session_state.data is None:
        st.warning("⚠️ 请先上传数据文件")
        st.stop()
    
    df = st.session_state.data
    
    # 筛选器
    st.markdown("### 🔧 筛选条件")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        # 获取所有大区并排序
        all_districts = sorted(df['大区'].dropna().unique().tolist())
        selected_districts = st.multiselect(
            "大区",
            options=all_districts,
            default=all_districts
        )
    
    with col2:
        # 根据选中的大区筛选省份
        if selected_districts:
            filtered_provinces = sorted(df[df['大区'].isin(selected_districts)]['省份'].dropna().unique().tolist())
        else:
            filtered_provinces = sorted(df['省份'].dropna().unique().tolist())
        
        selected_provinces = st.multiselect(
            "省份",
            options=filtered_provinces,
            default=filtered_provinces
        )
    
    with col3:
        # 根据选中的大区和省份筛选城市
        if selected_districts and selected_provinces:
            filtered_cities = sorted(df[
                (df['大区'].isin(selected_districts)) & 
                (df['省份'].isin(selected_provinces))
            ]['城市'].dropna().unique().tolist())
        else:
            filtered_cities = sorted(df['城市'].dropna().unique().tolist())
        
        selected_cities = st.multiselect(
            "城市",
            options=filtered_cities,
            default=filtered_cities
        )
    
    with col4:
        # 项目类型
        project_types = sorted(df['项目类型'].dropna().unique().tolist())
        selected_types = st.multiselect(
            "项目类型",
            options=project_types,
            default=project_types
        )
    
    with col5:
        # 创建季度选项
        quarter_options = [quarter_display_names.get(q, q) for q in quarters]
        selected_quarters = st.multiselect(
            "季度（仅影响城市分析）",
            options=quarter_options,
            default=['2025Q3']
        )
    
    # 应用筛选
    try:
        if selected_districts and selected_provinces and selected_cities and selected_types:
            filtered_df = df[
                (df['大区'].isin(selected_districts)) &
                (df['省份'].isin(selected_provinces)) &
                (df['城市'].isin(selected_cities)) &
                (df['项目类型'].isin(selected_types))
            ].copy()
        else:
            st.warning("请选择所有筛选条件")
            filtered_df = pd.DataFrame()
    except Exception as e:
        st.error(f"筛选数据时出错: {str(e)}")
        filtered_df = df.copy()
    
    # 显示筛选结果统计
    st.info(f"📊 当前筛选结果：共 {len(filtered_df)} 条记录")
    
    st.markdown("---")
    
    # Tab切换
    tab1, tab2 = st.tabs(["📊 汇总分析", "📍 单站分析"])
    
    with tab1:
        if len(filtered_df) == 0:
            st.warning("当前筛选条件下没有数据")
        else:
            st.markdown("### 📈 趋势分析")
            
            # 趋势计算函数
            def calculate_trend_flexible(row):
                # 获取所有季度数据，从后往前查找
                all_quarters = ['2025年三季度财务模型', '2025年二季度财务模型', '2025年一季度财务模型',
                              '2024年四季度财务模型', '2024年三季度财务模型', '2024年二季度财务模型', 
                              '2024年一季度财务模型']
                
                # 找到最新的有数据的季度
                current = None
                current_idx = -1
                for i, q in enumerate(all_quarters):
                    if q in row.index:
                        value = row[q]
                        if pd.notna(value) and str(value).strip() != '':
                            current = str(value).strip()
                            current_idx = i
                            break
                
                if current is None:
                    return '数据不足'
                
                # 找到上一个有数据的季度
                prev1 = None
                prev1_idx = -1
                for i in range(current_idx + 1, len(all_quarters)):
                    q = all_quarters[i]
                    if q in row.index:
                        value = row[q]
                        if pd.notna(value) and str(value).strip() != '':
                            prev1 = str(value).strip()
                            prev1_idx = i
                            break
                
                if prev1 is None:
                    return '数据不足'
                
                # 找到上上个有数据的季度
                prev2 = None
                for i in range(prev1_idx + 1, len(all_quarters)):
                    q = all_quarters[i]
                    if q in row.index:
                        value = row[q]
                        if pd.notna(value) and str(value).strip() != '':
                            prev2 = str(value).strip()
                            break
                
                # 将等级转换为数值进行比较
                grade_value = {'P10': 1, 'P30': 2, 'P50': 3, 'P90': 4}
                
                current_val = grade_value.get(current, 5)
                prev1_val = grade_value.get(prev1, 5)
                
                # 判断趋势
                if prev2:
                    prev2_val = grade_value.get(prev2, 5)
                    if current_val < prev1_val < prev2_val:
                        return '连续提升'
                    elif current_val > prev1_val > prev2_val:
                        return '连续下降'
                
                if current_val < prev1_val:
                    return '本季度提升'
                elif current_val > prev1_val:
                    return '本季度下降'
                else:
                    return '保持稳定'
            
            # 应用趋势计算
            filtered_df['趋势'] = filtered_df.apply(calculate_trend_flexible, axis=1)
            
            # 过滤掉数据不足的记录
            valid_df = filtered_df[filtered_df['趋势'] != '数据不足']
            
            # 统计各趋势数量
            if len(valid_df) > 0:
                trend_counts = valid_df['趋势'].value_counts()
                total = len(valid_df)
            else:
                trend_counts = pd.Series()
                total = 0
            
            # 显示趋势卡片
            col1, col2, col3, col4, col5 = st.columns(5)
            
            trends = ['本季度提升', '本季度下降', '保持稳定', '连续提升', '连续下降']
            cols = [col1, col2, col3, col4, col5]
            colors = ['🟢', '🔴', '🟡', '⬆️', '⬇️']
            
            for trend, col, color in zip(trends, cols, colors):
                with col:
                    count = trend_counts.get(trend, 0) if not trend_counts.empty else 0
                    percentage = (count / total * 100) if total > 0 else 0
                    st.metric(
                        label=f"{color} {trend}",
                        value=count,
                        delta=f"{percentage:.1f}%"
                    )
            
            # 显示数据不足的统计
            data_insufficient = len(filtered_df[filtered_df['趋势'] == '数据不足'])
            if data_insufficient > 0:
                st.info(f"ℹ️ 有 {data_insufficient} 个项目因数据不足无法计算趋势")
            
            st.markdown("---")
            
            # 城市分析
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🏆 优良率TOP10城市")
                
                # 动态查找最新有数据的季度
                latest_quarter = None
                for q in ['2025年三季度财务模型', '2025年二季度财务模型', '2025年一季度财务模型',
                         '2024年四季度财务模型', '2024年三季度财务模型']:
                    if q in filtered_df.columns:
                        non_empty = filtered_df[filtered_df[q].notna()]
                        if len(non_empty) > 0:
                            latest_quarter = q
                            break
                
                if latest_quarter:
                    st.caption(f"基于 {quarter_display_names.get(latest_quarter, latest_quarter)} 数据")
                    
                    city_stats = []
                    for city in filtered_df['城市'].unique():
                        city_df = filtered_df[filtered_df['城市'] == city]
                        valid_data = city_df[city_df[latest_quarter].notna()]
                        
                        if len(valid_data) >= 5:  # 降低门槛到5
                            counts = valid_data[latest_quarter].value_counts()
                            p10_p30 = counts.get('P10', 0) + counts.get('P30', 0)
                            total_valid = counts.sum()
                            if total_valid > 0:
                                rate = (p10_p30 / total_valid * 100)
                                city_stats.append({
                                    '城市': city,
                                    '站点数': total_valid,
                                    '优良率': f"{rate:.1f}%",
                                    '_rate': rate
                                })
                    
                    if city_stats:
                        city_stats_df = pd.DataFrame(city_stats)
                        city_stats_df = city_stats_df.sort_values('_rate', ascending=False).head(10)
                        city_stats_df['排名'] = range(1, len(city_stats_df) + 1)
                        st.dataframe(
                            city_stats_df[['排名', '城市', '站点数', '优良率']],
                            hide_index=True,
                            use_container_width=True
                        )
                    else:
                        st.info("暂无符合条件的城市（站点数>=5）")
                else:
                    st.warning("没有找到有效的季度数据")
            
            with col2:
                st.markdown("### ⚠️ 需关注城市TOP10")
                
                if latest_quarter:
                    st.caption(f"基于 {quarter_display_names.get(latest_quarter, latest_quarter)} 数据")
                    
                    concern_stats = []
                    for city in filtered_df['城市'].unique():
                        city_df = filtered_df[filtered_df['城市'] == city]
                        valid_data = city_df[city_df[latest_quarter].notna()]
                        
                        if len(valid_data) >= 5:  # 降低门槛
                            counts = valid_data[latest_quarter].value_counts()
                            p50_p90 = counts.get('P50', 0) + counts.get('P90', 0)
                            total_valid = counts.sum()
                            if total_valid > 0:
                                rate = (p50_p90 / total_valid * 100)
                                concern_stats.append({
                                    '城市': city,
                                    '站点数': total_valid,
                                    '需关注率': f"{rate:.1f}%",
                                    '_rate': rate
                                })
                    
                    if concern_stats:
                        concern_stats_df = pd.DataFrame(concern_stats)
                        concern_stats_df = concern_stats_df.sort_values('_rate', ascending=False).head(10)
                        concern_stats_df['排名'] = range(1, len(concern_stats_df) + 1)
                        st.dataframe(
                            concern_stats_df[['排名', '城市', '站点数', '需关注率']],
                            hide_index=True,
                            use_container_width=True
                        )
                    else:
                        st.info("暂无符合条件的城市（站点数>=5）")
                else:
                    st.warning("没有找到有效的季度数据")
    
    with tab2:
        st.markdown("### 📍 单站财务指标分析")
        
        if len(filtered_df) == 0:
            st.warning("当前筛选条件下没有数据，请调整筛选条件")
        else:
            st.caption(f"共筛选出 {len(filtered_df)} 个站点")
            
            # 准备单站数据
            station_data = []
            
            for idx, row in filtered_df.iterrows():
                try:
                    # 获取项目基本信息
                    project_name = str(row.get('项目名称', '未知项目'))
                    city = str(row.get('城市', '未知城市'))
                    district = str(row.get('大区', '未知大区'))
                    
                    # 获取各季度等级
                    grades = {}
                    for q in quarters:
                        if q in row.index:
                            value = row[q]
                            if pd.notna(value) and str(value).strip() != '':
                                grades[quarter_display_names.get(q, q)] = str(value).strip()
                            else:
                                grades[quarter_display_names.get(q, q)] = '-'
                        else:
                            grades[quarter_display_names.get(q, q)] = '-'
                    
                    # 获取上会和累计等级
                    meeting_grade = '-'
                    if '上会-财务模型' in row.index:
                        mg = row['上会-财务模型']
                        if pd.notna(mg) and str(mg).strip() != '':
                            meeting_grade = str(mg).strip()
                    
                    cumulative_grade = '-'
                    if '累计-财务模型' in row.index:
                        cg = row['累计-财务模型']
                        if pd.notna(cg) and str(cg).strip() != '':
                            cumulative_grade = str(cg).strip()
                    
                    # 判断累计趋势
                    trend = '-'
                    if meeting_grade != '-' and cumulative_grade != '-':
                        grade_value = {'P10': 1, 'P30': 2, 'P50': 3, 'P90': 4}
                        meeting_val = grade_value.get(meeting_grade, 5)
                        cumulative_val = grade_value.get(cumulative_grade, 5)
                        
                        if cumulative_val < meeting_val:
                            trend = '↑ 上升'
                        elif cumulative_val > meeting_val:
                            trend = '↓ 下降'
                        else:
                            trend = '→ 平稳'
                    
                    # 添加变化箭头
                    def add_arrow(current, previous):
                        if current == '-' or previous == '-':
                            return current
                        grade_value = {'P10': 1, 'P30': 2, 'P50': 3, 'P90': 4}
                        curr_val = grade_value.get(current, 5)
                        prev_val = grade_value.get(previous, 5)
                        
                        if curr_val < prev_val:
                            return f"{current} ↑"
                        elif curr_val > prev_val:
                            return f"{current} ↓"
                        else:
                            return current
                    
                    # 处理各季度数据，添加变化箭头
                    processed_grades = {}
                    prev_grade = meeting_grade
                    
                    for q_display in quarter_display_names.values():
                        current_grade = grades.get(q_display, '-')
                        if current_grade != '-' and prev_grade != '-':
                            processed_grades[q_display] = add_arrow(current_grade, prev_grade)
                            prev_grade = current_grade
                        else:
                            processed_grades[q_display] = current_grade
                            if current_grade != '-':
                                prev_grade = current_grade
                    
                    station_data.append({
                        '序号': len(station_data) + 1,
                        '场站名称': project_name,
                        '城市': city,
                        '大区': district,
                        '上会等级': meeting_grade,
                        **processed_grades,
                        '累计等级': cumulative_grade,
                        '趋势': trend
                    })
                except Exception as e:
                    st.error(f"处理数据时出错: {str(e)}")
                    continue
            
            # 显示表格
            if station_data:
                station_df = pd.DataFrame(station_data)
                
                st.success(f"✅ 成功加载 {len(station_df)} 条数据")
                
                # 使用st.dataframe显示
                st.dataframe(
                    station_df,
                    hide_index=True,
                    use_container_width=True,
                    height=600
                )
                
                # 添加下载按钮
                csv = station_df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 下载数据",
                    data=csv,
                    file_name=f'单站分析_{datetime.now().strftime("%Y%m%d")}.csv',
                    mime='text/csv'
                )
            else:
                st.warning("没有可显示的数据，请检查数据格式或筛选条件")

# 运行提示
if st.session_state.data is None and page != "📤 数据上传":
    st.info("请先在 [数据上传] 页面上传Excel数据文件")