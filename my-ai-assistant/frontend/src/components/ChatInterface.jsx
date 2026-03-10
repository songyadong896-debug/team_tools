import React, { useState, useRef, useEffect } from 'react';
import { sendMessage } from '../services/api';

const ChatInterface = () => {
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content: '你好！我是小智，你的智能助手。我可以帮你：\n\n📚 学习和记住知识（输入：/学习 [内容]）\n✅ 管理待办事项（输入：/待办 [事项]）\n📊 分析使用数据（输入：/分析）\n💬 日常对话交流\n\n请问有什么可以帮到你的吗？',
      timestamp: new Date().toISOString()
    }
  ]);
  const [inputMessage, setInputMessage] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [conversationId, setConversationId] = useState(null);
  const messagesEndRef = useRef(null);

  // 自动滚动到底部
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // 发送消息
  const handleSendMessage = async (e) => {
    e.preventDefault();

    if (!inputMessage.trim() || isLoading) return;

    const userMessage = inputMessage.trim();
    setInputMessage('');

    // 添加用户消息到界面
    const newUserMessage = {
      role: 'user',
      content: userMessage,
      timestamp: new Date().toISOString()
    };

    setMessages(prev => [...prev, newUserMessage]);
    setIsLoading(true);

    try {
      // 调用API
      const response = await sendMessage(userMessage, conversationId);

      // 保存对话ID
      if (!conversationId) {
        setConversationId(response.conversation_id);
      }

      // 添加AI回复到界面
      const aiMessage = {
        role: 'assistant',
        content: response.response,
        timestamp: response.timestamp
      };

      setMessages(prev => [...prev, aiMessage]);

    } catch (error) {
      console.error('发送消息失败:', error);

      // 显示错误消息
      const errorMessage = {
        role: 'assistant',
        content: '抱歉，发送消息时出现错误。请检查后端服务是否正常运行。',
        timestamp: new Date().toISOString(),
        isError: true
      };

      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  // 格式化时间
  const formatTime = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('zh-CN', {
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  // 清空对话
  const handleClearChat = () => {
    if (window.confirm('确定要清空对话记录吗？')) {
      setMessages([{
        role: 'assistant',
        content: '对话已清空。有什么可以帮到你的吗？',
        timestamp: new Date().toISOString()
      }]);
      setConversationId(null);
    }
  };

  return (
    <div className="flex flex-col h-screen bg-gradient-to-br from-blue-50 to-indigo-50">
      {/* 头部 */}
      <div className="bg-white shadow-md px-6 py-4 flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <div className="w-10 h-10 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-full flex items-center justify-center text-white font-bold text-lg">
            AI
          </div>
          <div>
            <h1 className="text-xl font-bold text-gray-800">我的智能助手</h1>
            <p className="text-sm text-gray-500">小智为你服务</p>
          </div>
        </div>
        <button
          onClick={handleClearChat}
          className="px-4 py-2 text-sm text-gray-600 hover:text-gray-800 hover:bg-gray-100 rounded-lg transition-colors"
        >
          清空对话
        </button>
      </div>

      {/* 消息区域 */}
      <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
        {messages.map((message, index) => (
          <div
            key={index}
            className={`fade-in flex ${
              message.role === 'user' ? 'justify-end' : 'justify-start'
            }`}
          >
            <div
              className={`max-w-2xl px-4 py-3 rounded-2xl ${
                message.role === 'user'
                  ? 'bg-blue-500 text-white'
                  : message.isError
                  ? 'bg-red-100 text-red-800'
                  : 'bg-white text-gray-800 shadow-md'
              }`}
            >
              <div className="whitespace-pre-wrap break-words">
                {message.content}
              </div>
              <div
                className={`text-xs mt-2 ${
                  message.role === 'user' ? 'text-blue-100' : 'text-gray-400'
                }`}
              >
                {formatTime(message.timestamp)}
              </div>
            </div>
          </div>
        ))}

        {/* 加载中提示 */}
        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-white px-4 py-3 rounded-2xl shadow-md">
              <div className="flex space-x-2">
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce"></div>
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.1s' }}></div>
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* 输入区域 */}
      <div className="bg-white border-t border-gray-200 px-6 py-4">
        <form onSubmit={handleSendMessage} className="flex space-x-4">
          <input
            type="text"
            value={inputMessage}
            onChange={(e) => setInputMessage(e.target.value)}
            placeholder="输入消息... (试试 /学习 或 /待办 命令)"
            className="flex-1 px-4 py-3 border border-gray-300 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            disabled={isLoading}
          />
          <button
            type="submit"
            disabled={!inputMessage.trim() || isLoading}
            className="px-6 py-3 bg-blue-500 text-white rounded-xl hover:bg-blue-600 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors font-medium"
          >
            发送
          </button>
        </form>

        {/* 快捷命令提示 */}
        <div className="mt-3 flex flex-wrap gap-2">
          <button
            onClick={() => setInputMessage('/待办列表')}
            className="px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-lg transition-colors"
          >
            查看待办
          </button>
          <button
            onClick={() => setInputMessage('/分析')}
            className="px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-lg transition-colors"
          >
            使用统计
          </button>
          <button
            onClick={() => setInputMessage('/学习 ')}
            className="px-3 py-1 text-sm bg-gray-100 hover:bg-gray-200 text-gray-700 rounded-lg transition-colors"
          >
            教它知识
          </button>
        </div>
      </div>
    </div>
  );
};

export default ChatInterface;
