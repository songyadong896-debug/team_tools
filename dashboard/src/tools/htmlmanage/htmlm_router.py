import json
import uuid
import os
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse

# 创建路由器
router = APIRouter()

# --- 配置和常量 ---
# 注意：这里的路径是相对于运行 main.py 的根目录
HTML_DATA_DIR = Path("src/tools/htmlmanage/data")
HTML_FILES_DIR = HTML_DATA_DIR / "html_files"
HTML_METADATA_FILE = HTML_DATA_DIR / "html_metadata.json"

# 确保目录存在
HTML_FILES_DIR.mkdir(parents=True, exist_ok=True)

# --- 辅助函数 ---

def init_html_metadata_file():
    """初始化元数据文件"""
    if not HTML_METADATA_FILE.exists():
        with open(HTML_METADATA_FILE, 'w') as f:
            json.dump({}, f)

# 确保模块加载时文件存在（替代原 main.py lifespan 中的逻辑）
init_html_metadata_file()

def load_html_metadata():
    """加载HTML文件元数据"""
    try:
        with open(HTML_METADATA_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_html_metadata(metadata):
    """保存HTML文件元数据"""
    with open(HTML_METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=2)

# --- 路由定义 (原 @app 改为 @router) ---

@router.get("/htmlmanage", response_class=HTMLResponse)
async def html_manage_page():
    """返回HTML文件管理页面"""
    try:
        # 路径指向 src/tools/htmlmanage/htmlmanage.html
        html_path = Path("src/tools/htmlmanage/htmlmanage.html")
        with open(html_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        return html_content
    except FileNotFoundError:
        return HTMLResponse(content="<h1>管理页面未找到 (src/tools/htmlmanage/htmlmanage.html)</h1>", status_code=404)

@router.post("/api/html/upload")
async def upload_html_file(file: UploadFile = File(...)):
    """上传HTML文件"""
    if not file.filename.endswith('.html'):
        raise HTTPException(status_code=400, detail="只允许上传HTML文件")

    # 读取文件内容
    content = await file.read()

    # 文件大小限制（5MB）
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="文件大小不能超过5MB")

    # 生成唯一ID
    file_id = str(uuid.uuid4())[:8]
    filename = f"{file_id}.html"
    filepath = HTML_FILES_DIR / filename

    # 保存文件
    with open(filepath, 'wb') as f:
        f.write(content)

    # 更新元数据
    metadata = load_html_metadata()
    metadata[file_id] = {
        "file_id": file_id,
        "filename": filename,
        "original_filename": file.filename,
        "created_at": datetime.now().isoformat(),
        "file_size": len(content),
        "view_count": 0
    }
    save_html_metadata(metadata)

    return {
        "success": True,
        "file_id": file_id,
        "url": f"/view/{file_id}",
        "message": "文件上传成功"
    }

@router.get("/api/html/files")
async def list_html_files():
    """获取所有HTML文件列表"""
    metadata = load_html_metadata()
    files = []

    for file_id, info in metadata.items():
        # 检查文件是否存在
        filepath = HTML_FILES_DIR / info['filename']
        if filepath.exists():
            files.append(info)

    # 按创建时间倒序排序
    files.sort(key=lambda x: x['created_at'], reverse=True)
    return files

@router.delete("/api/html/files/{file_id}")
async def delete_html_file(file_id: str):
    """删除HTML文件"""
    metadata = load_html_metadata()

    if file_id not in metadata:
        raise HTTPException(status_code=404, detail="文件不存在")

    # 删除文件
    file_info = metadata[file_id]
    filepath = HTML_FILES_DIR / file_info['filename']

    if filepath.exists():
        filepath.unlink()

    # 删除元数据
    del metadata[file_id]
    save_html_metadata(metadata)

    return {"success": True, "message": "文件删除成功"}

@router.get("/view/{file_id}", response_class=HTMLResponse)
async def view_html_file(file_id: str):
    """查看HTML文件"""
    metadata = load_html_metadata()

    if file_id not in metadata:
        raise HTTPException(status_code=404, detail="文件不存在")

    file_info = metadata[file_id]
    filepath = HTML_FILES_DIR / file_info['filename']

    if not filepath.exists():
        raise HTTPException(status_code=404, detail="文件不存在")

    # 更新访问次数
    metadata[file_id]['view_count'] += 1
    save_html_metadata(metadata)

    # 返回HTML内容
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    return HTMLResponse(content=content)

@router.get("/download/{file_id}")
async def download_html_file(file_id: str):
    """下载HTML文件"""
    metadata = load_html_metadata()

    if file_id not in metadata:
        raise HTTPException(status_code=404, detail="文件不存在")

    file_info = metadata[file_id]
    filepath = HTML_FILES_DIR / file_info['filename']

    if not filepath.exists():
        raise HTTPException(status_code=404, detail="文件不存在")

    return FileResponse(
        filepath,
        filename=file_info['original_filename'],
        media_type='text/html'
    )

# [新增] 提供 CSS 文件
@router.get("/htmlmanage/styles.css")
async def get_htmlmanage_css():
    css_path = Path("src/tools/htmlmanage/styles.css")
    if css_path.exists():
        return FileResponse(css_path, media_type="text/css")
    return HTMLResponse(content="/* CSS not found */", status_code=404)

# [新增] 提供 JSX 文件
@router.get("/htmlmanage/App.jsx")
async def get_htmlmanage_app():
    jsx_path = Path("src/tools/htmlmanage/App.jsx")
    if jsx_path.exists():
        return FileResponse(jsx_path, media_type="text/javascript") # 或者 application/javascript
    return HTMLResponse(content="// App.jsx not found", status_code=404)