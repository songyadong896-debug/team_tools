const { useState, useEffect, useRef } = React;

function App() {
    const [files, setFiles] = useState([]);
    const [loading, setLoading] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [message, setMessage] = useState(null); // { type: 'success' | 'error', text: '' }
    const [isDragOver, setIsDragOver] = useState(false);
    const fileInputRef = useRef(null);

    // 初始化加载
    useEffect(() => {
        loadFiles();
    }, []);

    // 自动清除消息
    useEffect(() => {
        if (message) {
            const timer = setTimeout(() => setMessage(null), 3000);
            return () => clearTimeout(timer);
        }
    }, [message]);

    const loadFiles = async () => {
        setLoading(true);
        try {
            const response = await fetch('/api/html/files');
            const data = await response.json();
            setFiles(data);
        } catch (error) {
            showMessage('error', '加载失败：' + error.message);
        } finally {
            setLoading(false);
        }
    };

    const showMessage = (type, text) => {
        setMessage({ type, text });
    };

    const handleFileUpload = async (uploadFiles) => {
        for (const file of uploadFiles) {
            if (!file.name.endsWith('.html')) {
                showMessage('error', `${file.name} 不是HTML文件`);
                continue;
            }

            const formData = new FormData();
            formData.append('file', file);

            try {
                const response = await fetch('/api/html/upload', {
                    method: 'POST',
                    body: formData
                });
                const data = await response.json();

                if (response.ok) {
                    showMessage('success', `${file.name} 上传成功！`);
                } else {
                    showMessage('error', data.detail || '上传失败');
                }
            } catch (error) {
                showMessage('error', '网络错误：' + error.message);
            }
        }
        loadFiles();
    };

    const handleDelete = async (fileId) => {
        if (!confirm('确定要删除这个文件吗？')) return;

        try {
            const response = await fetch(`/api/html/files/${fileId}`, {
                method: 'DELETE'
            });
            if (response.ok) {
                showMessage('success', '文件删除成功！');
                loadFiles();
            } else {
                showMessage('error', '删除失败');
            }
        } catch (error) {
            showMessage('error', '网络错误：' + error.message);
        }
    };

    const handleCopyLink = (fileId) => {
        const url = window.location.origin + `/view/${fileId}`;
        navigator.clipboard.writeText(url).then(() => {
            showMessage('success', '链接已复制到剪贴板！');
        }).catch(() => {
            const input = document.createElement('input');
            input.value = url;
            document.body.appendChild(input);
            input.select();
            document.execCommand('copy');
            document.body.removeChild(input);
            showMessage('success', '链接已复制到剪贴板！');
        });
    };

    // 格式化工具函数
    const formatFileSize = (bytes) => {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB';
        return (bytes / 1024 / 1024).toFixed(2) + ' MB';
    };

    const formatDate = (dateString) => {
        const date = new Date(dateString);
        const now = new Date();
        const diff = now - date;
        if (diff < 24 * 60 * 60 * 1000 && date.getDate() === now.getDate()) {
            return '今天 ' + date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
        }
        if (diff < 48 * 60 * 60 * 1000 && date.getDate() === now.getDate() - 1) {
            return '昨天 ' + date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
        }
        return date.toLocaleString('zh-CN');
    };

    // 过滤文件
    const filteredFiles = files.filter(file => 
        file.original_filename.toLowerCase().includes(searchTerm.toLowerCase()) ||
        file.file_id.toLowerCase().includes(searchTerm.toLowerCase())
    );

    // 统计数据
    const totalSize = files.reduce((acc, file) => acc + file.file_size, 0);
    const totalViews = files.reduce((acc, file) => acc + file.view_count, 0);

    return (
        <div className="container">
            {message && (
                <div className={`message ${message.type}`}>
                    {message.text}
                </div>
            )}

            <a href="/" className="back-link">← 返回首页</a>
            
            <div className="header">
                <h1><span>📄</span> HTML文件管理系统</h1>
            </div>

            <div className="stats">
                <div className="stat-card">
                    <div className="stat-number">{files.length}</div>
                    <div className="stat-label">文件总数</div>
                </div>
                <div className="stat-card">
                    <div className="stat-number">{formatFileSize(totalSize)}</div>
                    <div className="stat-label">总大小</div>
                </div>
                <div className="stat-card">
                    <div className="stat-number">{totalViews}</div>
                    <div className="stat-label">总访问量</div>
                </div>
            </div>

            <div className="upload-section">
                <div 
                    className={`upload-area ${isDragOver ? 'dragover' : ''}`}
                    onDragOver={(e) => { e.preventDefault(); setIsDragOver(true); }}
                    onDragLeave={() => setIsDragOver(false)}
                    onDrop={(e) => {
                        e.preventDefault();
                        setIsDragOver(false);
                        handleFileUpload(Array.from(e.dataTransfer.files));
                    }}
                >
                    <div className="upload-icon">📤</div>
                    <h3>上传HTML文件</h3>
                    <p style={{margin: '10px 0', color: '#666'}}>拖拽文件到这里或点击选择文件</p>
                    <p style={{fontSize: '12px', color: '#999'}}>支持批量上传，单个文件不超过5MB</p>
                    <input 
                        type="file" 
                        ref={fileInputRef}
                        accept=".html" 
                        multiple 
                        onChange={(e) => {
                            handleFileUpload(Array.from(e.target.files));
                            e.target.value = '';
                        }}
                    />
                    <button className="btn btn-primary" onClick={() => fileInputRef.current.click()}>
                        选择文件
                    </button>
                </div>
            </div>

            <div className="file-list">
                <div className="file-list-header">
                    <h2>文件列表</h2>
                    <div style={{display: 'flex', alignItems: 'center', gap: '10px'}}>
                        <input 
                            type="text" 
                            className="search-box" 
                            placeholder="搜索文件名..." 
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                        />
                        <span className="file-count">共 {filteredFiles.length} 个文件</span>
                    </div>
                </div>

                {loading ? (
                    <div className="loading">
                        <div className="loading-spinner"></div>
                        <p style={{marginTop: '10px'}}>加载中...</p>
                    </div>
                ) : filteredFiles.length === 0 ? (
                    <div className="empty-state">
                        <div className="empty-state-icon">📁</div>
                        <p>暂无文件</p>
                        <p style={{fontSize: '14px', marginTop: '10px'}}>上传HTML文件开始使用</p>
                    </div>
                ) : (
                    <table>
                        <thead>
                            <tr>
                                <th>文件名</th>
                                <th>大小</th>
                                <th>上传时间</th>
                                <th>访问次数</th>
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody>
                            {filteredFiles.map(file => (
                                <tr key={file.file_id}>
                                    <td>
                                        <div>{file.original_filename}</div>
                                        <div style={{fontSize: '12px', color: '#999'}}>ID: {file.file_id}</div>
                                    </td>
                                    <td className="file-size">{formatFileSize(file.file_size)}</td>
                                    <td>{formatDate(file.created_at)}</td>
                                    <td><span className="view-count">{file.view_count} 次</span></td>
                                    <td className="actions">
                                        <a href={`/view/${file.file_id}`} target="_blank" className="btn btn-primary">查看</a>
                                        <button onClick={() => handleCopyLink(file.file_id)} className="btn btn-info">复制链接</button>
                                        <a href={`/download/${file.file_id}`} className="btn btn-success">下载</a>
                                        <button onClick={() => handleDelete(file.file_id)} className="btn btn-danger">删除</button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    );
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<App />);