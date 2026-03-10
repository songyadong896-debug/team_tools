# src/dashboard/pxxdash/pxx_api.py
from function_datahandle.datadb_manager import DataDBManager
from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Optional
from datetime import datetime

import sys
from pathlib import Path
# 添加根目录到系统路径
root_path = Path(__file__).parent.parent.parent.parent
sys.path.append(str(root_path))


router = APIRouter(prefix="/api/pxxdash", tags=["PXX Dashboard"])


@router.get("/data")
async def get_dashboard_data(
    quarters: Optional[List[str]] = Query(
        None, description="指定季度列表，如['2024Q1','2024Q2']")
):
    """
    获取看板数据
    - 如果不指定quarters，返回所有季度的最新数据
    - 返回的数据已按项目合并，每个项目包含所有季度的信息
    """
    try:
        # SQL查询：获取每个季度的最新版本数据
        query = """
        WITH latest_versions AS (
            SELECT quarter, MAX(version_id) as latest_version
            FROM lcp_business_management.pxx_dashboard_data
            WHERE 1=1
            {quarter_filter}
            GROUP BY quarter
        )
        SELECT 
            d.id,
            d.version_id,
            d.region,
            d.province,
            d.city,
            d.project_type,
            d.bd,
            d.product_type,
            d.gun_count,
            d.project_id,
            d.project_name,
            d.approved_model,
            d.cumulative_model,
            d.current_quarter_model,
            d.quarter,
            d.data_version,
            d.upload_time,
            d.update_time
        FROM lcp_business_management.pxx_dashboard_data d
        INNER JOIN latest_versions lv 
            ON d.quarter = lv.quarter 
            AND d.version_id = lv.latest_version
        ORDER BY d.project_id, d.quarter
        """

        # 构建季度过滤条件
        quarter_filter = ""
        params = {}
        if quarters:
            quarter_placeholders = [
                f":quarter{i}" for i in range(len(quarters))]
            quarter_filter = f"AND quarter IN ({','.join(quarter_placeholders)})"
            for i, quarter in enumerate(quarters):
                params[f"quarter{i}"] = quarter

        # 执行查询
        final_query = query.format(quarter_filter=quarter_filter)
        rows = await DataDBManager.execute_query(final_query, params)

        # 按项目合并数据
        merged_data = merge_project_data(rows)

        return {
            "success": True,
            "data": merged_data,
            "total": len(merged_data)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# @router.get("/quarters")
# async def get_available_quarters():
#     """获取所有可用的季度列表"""
#     try:
#         query = """
#         SELECT DISTINCT quarter
#         FROM lcp_business_management.pxx_dashboard_data
#         ORDER BY quarter
#         """
#         rows = await DataDBManager.execute_query(query)
#         quarters = [row['quarter'] for row in rows]

#         # 生成从2024Q1到最新季度的完整列表
#         if quarters:
#             latest_quarter = max(quarters)  # 获取最新季度
#             quarter_list = generate_quarter_range('2024Q1', latest_quarter)

#             # 生成前端需要的季度配置格式
#             quarters_config = []
#             for q in quarter_list:
#                 quarters_config.append({
#                     'key': q.lower().replace('q', '_q_'),  # 如 'q1_2024'
#                     'label': q,  # 如 '2024Q1'
#                     'field': convert_quarter_to_field(q)  # 如 '2024年一季度财务模型'
#                 })

#         return {
#             "success": True,
#             "quarters": quarters,
#             "quartersConfig": quarters_config,
#             "latestQuarter": latest_quarter if quarters else None
#         }
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


@router.get("/quarters")
async def get_available_quarters():
    """获取所有可用的季度列表"""
    try:
        query = """
        SELECT DISTINCT quarter 
        FROM lcp_business_management.pxx_dashboard_data 
        ORDER BY quarter
        """
        rows = await DataDBManager.execute_query(query)
        quarters = [row['quarter'] for row in rows]

        print(f"查询到的季度: {quarters}")  # 添加日志

        # 初始化变量，避免作用域问题
        quarters_config = []
        latest_quarter = None

        # 生成从2024Q1到最新季度的完整列表
        if quarters:
            latest_quarter = max(quarters)  # 获取最新季度
            print(f"最新季度: {latest_quarter}")  # 添加日志

            try:
                quarter_list = generate_quarter_range('2024Q1', latest_quarter)
                print(f"生成的季度范围: {quarter_list}")  # 添加日志

                # 生成前端需要的季度配置格式
                for q in quarter_list:
                    config_item = {
                        'key': q.lower().replace('q', '_q_'),  # 如 'q1_2024'
                        'label': q,  # 如 '2024Q1'
                        # 如 '2024年一季度财务模型'
                        'field': convert_quarter_to_field(q)
                    }
                    quarters_config.append(config_item)
                    print(f"添加配置: {config_item}")  # 添加日志

            except Exception as e:
                print(f"生成季度配置时出错: {str(e)}")  # 捕获内部异常
                import traceback
                traceback.print_exc()

        result = {
            "success": True,
            "quarters": quarters,
            "quartersConfig": quarters_config,
            "latestQuarter": latest_quarter
        }

        print(f"最终返回结果: {result}")  # 添加日志

        return result

    except Exception as e:
        print(f"外层异常: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_dashboard_summary():
    """获取看板汇总统计信息"""
    try:
        query = """
        WITH latest_data AS (
            SELECT d.*
            FROM lcp_business_management.pxx_dashboard_data d
            INNER JOIN (
                SELECT quarter, MAX(version_id) as latest_version
                FROM lcp_business_management.pxx_dashboard_data
                GROUP BY quarter
            ) lv ON d.quarter = lv.quarter AND d.version_id = lv.latest_version
        )
        SELECT 
            COUNT(DISTINCT project_id) as total_projects,
            COUNT(DISTINCT CASE WHEN current_quarter_model IN ('P10', 'P30') THEN project_id END) as excellent_projects,
            COUNT(DISTINCT CASE WHEN current_quarter_model = 'P90' THEN project_id END) as p90_projects,
            SUM(gun_count) as total_guns,
            COUNT(DISTINCT quarter) as total_quarters,
            MAX(upload_time) as latest_upload_time
        FROM latest_data
        """

        result = await DataDBManager.execute_query(query)

        return {
            "success": True,
            "summary": result[0] if result else {}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_version_statistics():
    """
    获取每个季度的版本统计信息
    返回每个季度有哪些版本，每个版本有多少场站
    """
    try:
        query = """
        SELECT 
            quarter,
            version_id,
            COUNT(DISTINCT project_id) as station_count
        FROM lcp_business_management.pxx_dashboard_data
        GROUP BY quarter, version_id
        ORDER BY quarter DESC, version_id DESC
        """

        rows = await DataDBManager.execute_query(query)

        # 按季度组织数据
        statistics = {}

        for row in rows:
            quarter = row['quarter']
            version_id = row['version_id']
            count = row['station_count']

            if quarter not in statistics:
                statistics[quarter] = {}

            # 格式化版本名称（如果需要的话）
            version_name = format_version_name(version_id)
            statistics[quarter][version_name] = count

        return {
            "success": True,
            "data": statistics
        }

    except Exception as e:
        print(f"获取版本统计时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/check-quarter/{quarter}")
async def check_quarter_exists(quarter: str):
    """
    检查指定季度是否已有数据
    返回该季度的版本数和场站数
    """
    try:
        query = """
        SELECT 
            COUNT(DISTINCT version_id) as version_count,
            COUNT(DISTINCT project_id) as station_count
        FROM lcp_business_management.pxx_dashboard_data
        WHERE quarter = :quarter
        """

        rows = await DataDBManager.execute_query(query, {"quarter": quarter})

        if rows and rows[0]['version_count'] > 0:
            return {
                "exists": True,
                "versionCount": rows[0]['version_count'],
                "stationCount": rows[0]['station_count']
            }
        else:
            return {
                "exists": False,
                "versionCount": 0,
                "stationCount": 0
            }

    except Exception as e:
        print(f"检查季度时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def format_version_name(version_id: str) -> str:
    """
    格式化版本ID为更友好的显示名称
    可以根据实际的版本ID格式进行调整
    """
    # 如果版本ID是时间戳格式，可以转换为日期
    try:
        # 假设version_id格式类似 "v20241115_143022"
        if version_id.startswith('v'):
            parts = version_id[1:].split('_')
            if len(parts) >= 2:
                date_str = parts[0]
                time_str = parts[1]
                # 格式化为 "2024-11-15 14:30:22"
                formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                return f"版本 {formatted}"

        # 如果是其他格式，直接返回
        return version_id

    except Exception:
        # 如果格式化失败，返回原始ID
        return version_id


def merge_project_data(data: List[Dict]) -> List[Dict]:
    """
    将同一项目的不同季度数据合并成一行
    优先使用最新季度的属性，如果最新季度没有该项目，则往前查找
    """
    # 1. 先整理数据：按季度分组
    quarters_data = {}
    for row in data:
        quarter = row['quarter']
        if quarter not in quarters_data:
            quarters_data[quarter] = {}
        quarters_data[quarter][row['project_id']] = row

    # 2. 获取所有项目ID和季度，并排序
    all_project_ids = set()
    all_quarters = sorted(quarters_data.keys(), reverse=True)  # 从新到旧排序

    for quarter_projects in quarters_data.values():
        all_project_ids.update(quarter_projects.keys())

    # 3. 为每个项目构建数据
    project_map = {}

    for project_id in all_project_ids:
        # 3.1 查找该项目最新的基础属性（从最新季度开始往前找）
        base_info = None
        base_info_quarter = None

        for quarter in all_quarters:
            if project_id in quarters_data[quarter]:
                base_info = quarters_data[quarter][project_id]
                base_info_quarter = quarter
                break

        if not base_info:
            continue  # 理论上不应该发生

        # 3.2 使用找到的基础信息初始化项目数据
        project_data = {
            'project_id': project_id,
            'project_name': base_info['project_name'],
            'region': base_info['region'],
            'province': base_info['province'],
            'city': base_info['city'],
            'project_type': base_info['project_type'],
            'bd': base_info['bd'],
            'product_type': base_info['product_type'],
            'gun_count': base_info['gun_count'],
            'approved_model': base_info['approved_model'],
            'cumulative_model': base_info['cumulative_model'],
            '_base_info_quarter': base_info_quarter,  # 记录基础信息来源季度
            '_quarters': []  # 记录该项目存在的季度
        }

        # 3.3 添加所有季度的财务模型数据
        for quarter in quarters_data:
            if project_id in quarters_data[quarter]:
                quarter_field = convert_quarter_to_field(quarter)
                project_data[quarter_field] = quarters_data[quarter][project_id]['current_quarter_model']
                project_data['_quarters'].append(quarter)

        # 3.4 如果累计和上会模型在最新数据中缺失，也向前查找
        if not project_data['cumulative_model']:
            for quarter in all_quarters:
                if project_id in quarters_data[quarter] and quarters_data[quarter][project_id].get('cumulative_model'):
                    project_data['cumulative_model'] = quarters_data[quarter][project_id]['cumulative_model']
                    break

        if not project_data['approved_model']:
            for quarter in all_quarters:
                if project_id in quarters_data[quarter] and quarters_data[quarter][project_id].get('approved_model'):
                    project_data['approved_model'] = quarters_data[quarter][project_id]['approved_model']
                    break

        project_map[project_id] = project_data

    # 4. 添加调试信息（可选）
    result = list(project_map.values())

    # 统计基础信息来源
    quarter_sources = {}
    for item in result:
        source = item.get('_base_info_quarter', 'unknown')
        quarter_sources[source] = quarter_sources.get(source, 0) + 1

    return result


def convert_quarter_to_field(quarter: str) -> str:
    """
    将季度代码转换为字段名
    2024Q1 -> 2024年一季度财务模型
    """
    quarter_map = {
        'Q1': '一季度',
        'Q2': '二季度',
        'Q3': '三季度',
        'Q4': '四季度'
    }

    try:
        year = quarter[:4]
        q = quarter[4:]

        if q in quarter_map:
            return f"{year}年{quarter_map[q]}财务模型"
    except:
        pass

    return f"{quarter}财务模型"  # 如果格式不匹配，返回带季度的默认格式


def generate_quarter_range(start_quarter: str, end_quarter: str) -> List[str]:
    """
    生成季度范围
    start_quarter: '2024Q1'
    end_quarter: '2025Q3'
    返回: ['2024Q1', '2024Q2', ..., '2025Q3']
    """
    quarters = []

    # 解析开始和结束季度
    start_year = int(start_quarter[:4])
    start_q = int(start_quarter[5])
    end_year = int(end_quarter[:4])
    end_q = int(end_quarter[5])

    current_year = start_year
    current_q = start_q

    while current_year < end_year or (current_year == end_year and current_q <= end_q):
        quarters.append(f"{current_year}Q{current_q}")

        current_q += 1
        if current_q > 4:
            current_q = 1
            current_year += 1

    return quarters
