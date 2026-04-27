/******************************************************************
 * Channel SDK - Channel Types
 * 
 * OpenClaw Channel SDK 的核心类型定义
 * 
 * @see https://github.com/chatu-ai/openclaw-web-hub-channel
 ******************************************************************/

/**
 * 连接状态 [Channel SDK 标准]
 */
export type ConnectionStatus =
  | 'connecting'   // 连接中
  | 'connected'   // 已连接
  | 'disconnected' // 已断开
  | 'error';      // 错误

/**
 * 消息类型 [Channel SDK 标准]
 */
export enum MessageType {
  TEXT = 'text',
  IMAGE = 'image',
  VIDEO = 'video',
  AUDIO = 'audio',
  FILE = 'file',
  LOCATION = 'location',
  /** Plugin-Channel Realtime: operation/command type */
  ACTION = 'action',
  /** Plugin-Channel Realtime: unrecognised type, frontend renders placeholder */
  UNKNOWN = 'unknown',
}

/**
 * 目标类型 [Channel SDK 标准]
 */
export enum TargetType {
  USER = 'user',
  GROUP = 'group',
  CHANNEL = 'channel',
}

/**
 * 消息发送者 [Channel SDK 标准]
 */
export interface Sender {
  /** 发送者 ID [Channel SDK 标准] */
  id: string;
  
  /** 发送者显示名 [Channel SDK 标准] */
  displayName?: string;
  
  /** 发送者头像 URL [Channel SDK 标准] */
  avatarUrl?: string;
  
  /** 是否为机器人 [Channel SDK 标准] */
  isBot?: boolean;
}

/**
 * 消息目标 [Channel SDK 标准]
 */
export interface Target {
  /** 目标类型 [Channel SDK 标准] */
  type: TargetType;
  
  /** 目标 ID [Channel SDK 标准] */
  id: string;
  
  /** 目标名称 [Channel SDK 标准] */
  name?: string;
}

/**
 * 媒体附件 [Channel SDK 标准]
 */
export interface Media {
  /** 媒体类型 [Channel SDK 标准] */
  type: MessageType;
  
  /** 媒体 URL [Channel SDK 标准] */
  url: string;
  
  /** MIME 类型 [Channel SDK 标准] */
  mimeType?: string;
  
  /** 文件大小（字节）[Channel SDK 标准] */
  size?: number;
  
  /** 图片/视频宽度 [Channel SDK 标准] */
  width?: number;
  
  /** 图片/视频高度 [Channel SDK 标准] */
  height?: number;
  
  /** 音视频时长（秒）[Channel SDK 标准] */
  duration?: number;
  
  /** 缩略图 URL [Channel SDK 标准] */
  thumbnailUrl?: string;
}

/**
 * 消息内容 [Channel SDK 标准]
 */
export interface MessageContent {
  /** 文本内容 [Channel SDK 标准] */
  text: string;
  
  /** 文本格式 [Channel SDK 标准] */
  format?: 'plain' | 'markdown' | 'html';
}

/**
 * 消息回复 [Channel SDK 标准]
 */
export interface MessageReply {
  /** 回复的消息 ID [Channel SDK 标准] */
  messageId: string;
  
  /** 引用的文本 [Channel SDK 标准] */
  quotedText?: string;
}

/**
 * 入站消息 [Channel SDK 标准]
 */
export interface InboundMessage {
  /** 消息 ID [Channel SDK 标准] */
  id: string;
  
  /** 通道 ID [Channel SDK 标准] */
  channelId: string;
  
  /** 发送者 [Channel SDK 标准] */
  sender: Sender;
  
  /** 目标 [Channel SDK 标准] */
  target: Target;
  
  /** 消息内容 [Channel SDK 标准] */
  content: MessageContent;
  
  /** 媒体附件 [Channel SDK 标准] */
  media?: Media[];
  
  /** 回复 [Channel SDK 标准] */
  replyTo?: MessageReply;
  
  /** 时间戳 [Channel SDK 标准] */
  timestamp: number;
  
  /** 元数据 [Channel SDK 标准] */
  metadata?: Record<string, unknown>;
}

/**
 * 出站消息 [Channel SDK 标准]
 */
export interface OutboundMessage {
  /** 消息 ID [Channel SDK 标准] */
  messageId?: string;
  
  /** 目标 [Channel SDK 标准] */
  target: Target;
  
  /** 消息内容 [Channel SDK 标准] */
  content: MessageContent;
  
  /** 媒体附件 [Channel SDK 标准] */
  media?: Media[];
  
  /** 回复 [Channel SDK 标准] */
  replyTo?: string;
  
  /** 消息标记 [Channel SDK 标准] */
  flags?: {
    /** 静默发送 [Channel SDK 标准] */
    silent?: boolean;
    /** 紧急消息 [Channel SDK 标准] */
    urgent?: boolean;
  };
  
  /** 元数据 [Channel SDK 标准] */
  metadata?: Record<string, unknown>;
}

/**
 * 发送结果 [Channel SDK 标准]
 */
export interface SendResult {
  /** 消息 ID [Channel SDK 标准] */
  messageId: string;
  
  /** 是否成功 [Channel SDK 标准] */
  success: boolean;
  
  /** 时间戳 [Channel SDK 标准] */
  timestamp: number;
  
  /** 错误信息 [Channel SDK 标准] */
  error?: {
    code: string;
    message: string;
  };
}

/**
 * 连接配置 [Channel SDK 标准]
 */
export interface ConnectionConfig {
  /** Channel ID [Channel SDK 标准] */
  channelId: string;
  
  /** 访问令牌 [Channel SDK 标准] */
  accessToken: string;
  
  /** WebHub Backend URL (配置时设置) */
  webhubUrl?: string;
  
  /** Webhook URL [Channel SDK 标准] */
  webhookUrl?: string;
  
  /** WebSocket URL [Channel SDK 标准] */
  wsUrl?: string;
  
  /** 心跳间隔（毫秒）[Channel SDK 标准] */
  heartbeatInterval?: number;
  
  /** 心跳超时（毫秒）[Channel SDK 标准] */
  heartbeatTimeout?: number;
  
  /** 最大重连次数 [Channel SDK 标准] */
  maxReconnectAttempts?: number;

  /** Plugin-Channel Realtime: connection mode, 'user' | 'group' (future) */
  mode?: 'user' | 'group';
}

/**
 * Channel 统计信息 [Channel SDK 标准]
 */
export interface ChannelStats {
  /** 发送消息数 [Channel SDK 标准] */
  messagesSent: number;
  
  /** 接收消息数 [Channel SDK 标准] */
  messagesReceived: number;
  
  /** 连接时长（秒）[Channel SDK 标准] */
  connectedDuration: number;
  
  /** 最后活跃时间 [Channel SDK 标准] */
  lastActiveAt: number;
  
  /** 连接模式 (WebHub 扩展) */
  mode?: string;
}

/**
 * Channel 能力 [Channel SDK 标准]
 */
export interface ChannelCapabilities {
  /** 支持的消息类型 [Channel SDK 标准] */
  messageTypes: MessageType[];
  
  /** 支持的目标类型 [Channel SDK 标准] */
  targetTypes: TargetType[];
  
  /** 支持富文本格式 [Channel SDK 标准] */
  richFormats?: ('markdown' | 'html')[];
  
  /** 支持附件 [Channel SDK 标准] */
  attachments?: boolean;
  
  /** 支持回复 [Channel SDK 标准] */
  reply?: boolean;
  
  /** 支持消息编辑 [Channel SDK 标准] */
  edit?: boolean;
  
  /** 支持消息删除 [Channel SDK 标准] */
  delete?: boolean;
  
  /** 支持表情反应 [Channel SDK 标准] */
  reactions?: boolean;
  
  /** 支持投票 [Channel SDK 标准] */
  polls?: boolean;
  
  /** 支持按钮 [Channel SDK 标准] */
  buttons?: boolean;
}

// ── T005 Plugin-Channel SSE: Streaming Types ─────────────────────────────────

/**
 * T005: Content type for multi-type message rendering (mirrors API MessageContentType).
 */
export type MessageContentType = 'text' | 'image' | 'file' | 'action' | 'unknown';

/**
 * T005: Payload for a single streaming chunk, sent to API via POST /api/channel/stream/chunk.
 */
export type StreamChunkType =
  | 'text'
  | 'reasoning'
  | 'tool_start'
  | 'tool_result'
  | 'tool_item'
  | 'tool_plan'
  | 'tool_approval'
  | 'tool_command_output'
  | 'tool_patch_summary';

export interface StreamChunkPayload {
  /** Unique message ID (UUID v4) */
  messageId: string;
  /** Monotonically increasing sequence number (0-based) */
  seq: number;
  /** Incremental text delta from this chunk */
  delta: string;
  /** Content type of this chunk */
  type?: StreamChunkType;
}

/**
 * T005: Payload for the streaming completion frame, POST /api/channel/stream/done.
 */
export interface StreamDonePayload {
  /** Same messageId as the preceding chunks */
  messageId: string;
  /** Total number of chunks sent (for frontend sequence validation) */
  totalSeq: number;
}

/**
 * T005: A single cached message entry on the plugin side.
 */
export interface PluginCacheEntry {
  /** Message ID (UUID v4) — idempotency key */
  messageId: string;
  /** Serialised outbound message payload */
  payload: string;
  /** Unix ms timestamp when cached */
  cachedAt: number;
  /** Whether the message has been acknowledged by the API */
  acked?: boolean;
}

/**
 * T005: Root structure of the plugin-side message cache JSON file
 * (default: ~/.openclaw/chatu/message_cache.json).
 */
export interface PluginMessageCacheFile {
  version: 1;
  entries: PluginCacheEntry[];
  updatedAt: number;
}
