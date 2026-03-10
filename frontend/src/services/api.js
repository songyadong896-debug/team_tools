import axios from 'axios';

// 根据环境自动选择API地址
// 开发环境使用localhost，生产环境使用环境变量或自动检测
const API_BASE_URL = import.meta.env.VITE_API_URL ||
  (import.meta.env.MODE === 'production'
    ? window.location.origin.replace(/https?:\/\/[^/]+/, 'YOUR_RENDER_URL_HERE')
    : 'http://localhost:8000');

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// 聊天API
export const sendMessage = async (message, conversationId = null) => {
  try {
    const response = await api.post('/api/chat', {
      message,
      conversation_id: conversationId,
    });
    return response.data;
  } catch (error) {
    console.error('发送消息失败:', error);
    throw error;
  }
};

// 知识API
export const learnKnowledge = async (content, category = 'general') => {
  try {
    const response = await api.post('/api/learn', {
      content,
      category,
    });
    return response.data;
  } catch (error) {
    console.error('学习知识失败:', error);
    throw error;
  }
};

export const getKnowledge = async () => {
  try {
    const response = await api.get('/api/knowledge');
    return response.data;
  } catch (error) {
    console.error('获取知识失败:', error);
    throw error;
  }
};

// 待办事项API
export const createTodo = async (title, description = '', priority = 'medium') => {
  try {
    const response = await api.post('/api/todos', {
      title,
      description,
      priority,
    });
    return response.data;
  } catch (error) {
    console.error('创建待办失败:', error);
    throw error;
  }
};

export const getTodos = async () => {
  try {
    const response = await api.get('/api/todos');
    return response.data;
  } catch (error) {
    console.error('获取待办列表失败:', error);
    throw error;
  }
};

export const completeTodo = async (todoId) => {
  try {
    const response = await api.put(`/api/todos/${todoId}/complete`);
    return response.data;
  } catch (error) {
    console.error('完成待办失败:', error);
    throw error;
  }
};

export const deleteTodo = async (todoId) => {
  try {
    const response = await api.delete(`/api/todos/${todoId}`);
    return response.data;
  } catch (error) {
    console.error('删除待办失败:', error);
    throw error;
  }
};

// 分析API
export const getAnalytics = async () => {
  try {
    const response = await api.get('/api/analytics');
    return response.data;
  } catch (error) {
    console.error('获取统计信息失败:', error);
    throw error;
  }
};

export default api;
