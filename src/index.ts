/**
 * OpenClaw Chatu Channel Plugin
 *
 * This plugin enables OpenClaw to communicate with Chatu/WebHub services
 * via HTTP polling (inbound) and HTTP POST (outbound).
 *
 * Architecture:
 *   User (browser) → WebHub service (POST /api/webhub/channels/:id/messages)
 *   Plugin polls    → GET /api/channel/messages/pending
 *   Plugin dispatches → OpenClaw AI
 *   AI responds     → plugin outbound.sendText → POST /api/channel/messages
 *   WebHub service  → WebSocket push → browser
 *
 * @see https://docs.openclaw.ai/channels/chatu
 * @see https://github.com/chatu-ai/openclaw-web-hub-channel
 */

import type {
  OpenClawPluginApi,
  ChannelPlugin,
  OpenClawConfig,
  ChannelAccountSnapshot,
  ChannelGatewayContext,
  ChannelLogoutContext,
  ChannelSetupInput,
  ChannelLogSink,
} from 'openclaw/plugin-sdk';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import pkg from '../package.json';
import { WebSocketAdapter } from './sdk/adapters/websocket';
import { MessageCache } from './sdk/adapters/cache';
import type { InboundMessage } from './sdk/types/channel';

/** Resolved per-account configuration for the Chatu channel. */
export interface ChatuAccount {
  accountId: string;
  apiUrl: string;
  channelId: string;
  secret?: string;
  accessToken?: string;
  timeout: number;
  streaming?: boolean;
  streamThrottleMs?: number;
}

/** Custom setup input fields used by the Chatu channel. */
type ChatuSetupInput = ChannelSetupInput & {
  apiUrl?: string;
  channelId?: string;
  secret?: string;
};

const CHANNEL_ID = 'chatu' as const;
const POLL_INTERVAL_MS = 2000;
const MAX_BACKOFF_MS = 30_000;
const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_CHUNK_LIMIT = 4000;
const DEFAULT_STREAM_THROTTLE_MS = 0;

type ChatuStreamChunkType =
  | 'text'
  | 'reasoning'
  | 'tool_start'
  | 'tool_result'
  | 'tool_item'
  | 'tool_plan'
  | 'tool_approval'
  | 'tool_command_output'
  | 'tool_patch_summary';
type ChatuToolStreamChunkType = Exclude<ChatuStreamChunkType, 'text' | 'reasoning'>;

type ChatuStreamFrame =
  | {
      kind: 'stream_chunk';
      messageId: string;
      seq: number;
      delta: string;
      type?: ChatuStreamChunkType;
      accountId?: string | null;
    }
  | {
      kind: 'stream_done';
      messageId: string;
      totalSeq: number;
      accountId?: string | null;
    };

type StreamRelayResult = { ok: boolean; error?: string };

export function createChatuStreamRelay(params: {
  messageId: string;
  accountId: string;
  throttleMs?: number;
  chunkLimit?: number;
  deliverStreamChunk: (frame: Extract<ChatuStreamFrame, { kind: 'stream_chunk' }>) => Promise<StreamRelayResult>;
  deliverStreamDone: (frame: Extract<ChatuStreamFrame, { kind: 'stream_done' }>) => Promise<StreamRelayResult>;
  cacheFrame?: (frame: ChatuStreamFrame, reason?: string) => void;
}) {
  const { messageId, accountId } = params;
  const throttleMs = params.throttleMs ?? DEFAULT_STREAM_THROTTLE_MS;
  const chunkLimit = Math.max(1, params.chunkLimit ?? DEFAULT_CHUNK_LIMIT);
  let seq = 0;
  let stopped = false;
  let degraded = false;
  let sendQueue: Promise<void> = Promise.resolve();
  let textFramesEmitted = 0;
  let successfulTextFrames = 0;

  const cacheFrame = (frame: ChatuStreamFrame, reason?: string) => {
    degraded = true;
    params.cacheFrame?.(frame, reason);
  };

  const enqueueFrame = (frame: ChatuStreamFrame): Promise<void> => {
    sendQueue = sendQueue.then(async () => {
      if (degraded) {
        cacheFrame(frame, 'stream relay degraded');
        return;
      }
      const result = frame.kind === 'stream_chunk'
        ? await params.deliverStreamChunk(frame)
        : await params.deliverStreamDone(frame);
      if (!result.ok) {
        cacheFrame(frame, result.error);
        return;
      }
      if (frame.kind === 'stream_chunk' && frame.type === 'text') {
        successfulTextFrames++;
      }
    }).catch((err) => {
      cacheFrame(frame, err instanceof Error ? err.message : String(err));
    });
    return sendQueue;
  };

  const makeThrottledSender = (
    getLastSent: () => string,
    setLastSent: (s: string) => void,
    type: 'text' | 'reasoning',
  ) => {
    let pending = '';
    let timer: ReturnType<typeof setTimeout> | undefined;
    let inFlight: Promise<void> | undefined;

    const flush = async () => {
      if (timer) { clearTimeout(timer); timer = undefined; }
      if (inFlight) { await inFlight; }
      if (!pending) return;
      const text = pending;
      pending = '';
      const delta = text.slice(getLastSent().length);
      if (!delta) return;
      setLastSent(text);
      const frame: Extract<ChatuStreamFrame, { kind: 'stream_chunk' }> = {
        kind: 'stream_chunk',
        messageId,
        seq: seq++,
        delta,
        type,
        accountId,
      };
      if (type === 'text') textFramesEmitted++;
      inFlight = enqueueFrame(frame).finally(() => { inFlight = undefined; });
      await inFlight;
    };

    return {
      update: (text: string) => {
        if (stopped) return;
        pending = text;
        if (throttleMs <= 0) {
          void flush();
          return;
        }
        if (!timer) timer = setTimeout(() => { void flush(); }, throttleMs);
      },
      flush,
    };
  };

  let lastText = '';
  let lastReasoning = '';
  const textSender = makeThrottledSender(() => lastText, s => { lastText = s; }, 'text');
  const reasoningSender = makeThrottledSender(() => lastReasoning, s => { lastReasoning = s; }, 'reasoning');

  const flushPendingText = async () => {
    await textSender.flush();
    await reasoningSender.flush();
  };

  return {
    updateText: (text: string) => textSender.update(text),
    updateReasoning: (text: string) => reasoningSender.update(text),
    sendChunkDirect: async (delta: string, type: ChatuToolStreamChunkType) => {
      if (!delta || stopped) return;
      await flushPendingText();
      await enqueueFrame({ kind: 'stream_chunk', messageId, seq: seq++, delta, type, accountId });
    },
    sendChunkedDirect: async (delta: string, type: ChatuToolStreamChunkType) => {
      if (!delta || stopped) return;
      await flushPendingText();
      for (let offset = 0; offset < delta.length; offset += chunkLimit) {
        await enqueueFrame({
          kind: 'stream_chunk',
          messageId,
          seq: seq++,
          delta: delta.slice(offset, offset + chunkLimit),
          type,
          accountId,
        });
      }
    },
    resetForNewMessage: async () => {
      await flushPendingText();
      lastText = '';
      lastReasoning = '';
    },
    finalize: async () => {
      await flushPendingText();
      await sendQueue;
      await enqueueFrame({ kind: 'stream_done', messageId, totalSeq: seq, accountId });
      await sendQueue;
      stopped = true;
    },
    hasTextFrames: () => textFramesEmitted > 0,
    hasSuccessfulTextFrames: () => successfulTextFrames > 0,
    isDegraded: () => degraded,
  };
}

function stringifyCompactJson(value: unknown): string | undefined {
  try {
    return JSON.stringify(value);
  } catch {
    return undefined;
  }
}

export function formatChatuToolStartPayload(payload: { name?: string; phase?: string } | undefined): string {
  const name = typeof payload?.name === 'string' && payload.name.trim() ? payload.name.trim() : 'tool';
  const phase = typeof payload?.phase === 'string' && payload.phase.trim() ? payload.phase.trim() : '';
  return phase ? `${name} ${phase}` : name;
}

export function formatChatuToolResultPayload(payload: any): string {
  const parts: string[] = [];
  const text = typeof payload?.text === 'string' ? payload.text : '';
  if (text) parts.push(text);

  const mediaUrls = [
    ...(typeof payload?.mediaUrl === 'string' && payload.mediaUrl ? [payload.mediaUrl] : []),
    ...(Array.isArray(payload?.mediaUrls) ? payload.mediaUrls.filter((url: unknown): url is string => typeof url === 'string' && url.length > 0) : []),
  ];
  if (mediaUrls.length > 0) {
    parts.push(`media: ${mediaUrls.join(', ')}`);
  }

  if (payload?.channelData && Object.keys(payload.channelData).length > 0) {
    const channelData = stringifyCompactJson(payload.channelData);
    if (channelData) parts.push(`channelData: ${channelData}`);
  }

  if (parts.length > 0) return parts.join('\n');
  return stringifyCompactJson(payload) ?? '';
}

function pushLine(parts: string[], label: string, value: unknown): void {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed) parts.push(`${label}: ${trimmed}`);
    return;
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    parts.push(`${label}: ${String(value)}`);
  }
}

function formatStringArray(label: string, values: unknown): string | undefined {
  if (!Array.isArray(values)) return undefined;
  const cleaned = values.filter((value): value is string => typeof value === 'string' && value.trim().length > 0);
  if (cleaned.length === 0) return undefined;
  return `${label}: ${cleaned.join(', ')}`;
}

export function formatChatuToolItemPayload(payload: any): string {
  const parts: string[] = [];
  const phase = typeof payload?.phase === 'string' && payload.phase.trim() ? payload.phase.trim() : 'item';
  const label =
    (typeof payload?.title === 'string' && payload.title.trim()) ||
    (typeof payload?.name === 'string' && payload.name.trim()) ||
    (typeof payload?.kind === 'string' && payload.kind.trim()) ||
    'tool item';
  parts.push(`${phase}: ${label}`);
  pushLine(parts, 'status', payload?.status);
  pushLine(parts, 'summary', payload?.summary);
  pushLine(parts, 'progress', payload?.progressText);
  pushLine(parts, 'itemId', payload?.itemId);
  pushLine(parts, 'approvalId', payload?.approvalId);
  pushLine(parts, 'approvalSlug', payload?.approvalSlug);
  return parts.join('\n');
}

export function formatChatuToolPlanPayload(payload: any): string {
  const parts: string[] = [];
  const phase = typeof payload?.phase === 'string' && payload.phase.trim() ? payload.phase.trim() : 'plan';
  const title = typeof payload?.title === 'string' && payload.title.trim() ? payload.title.trim() : 'plan update';
  parts.push(`${phase}: ${title}`);
  pushLine(parts, 'explanation', payload?.explanation);
  if (Array.isArray(payload?.steps)) {
    const steps = payload.steps.filter((step: unknown): step is string => typeof step === 'string' && step.trim().length > 0);
    if (steps.length > 0) parts.push(steps.map((step: string, index: number) => `${index + 1}. ${step}`).join('\n'));
  }
  pushLine(parts, 'source', payload?.source);
  return parts.join('\n');
}

export function formatChatuToolApprovalPayload(payload: any): string {
  const parts: string[] = [];
  const phase = typeof payload?.phase === 'string' && payload.phase.trim() ? payload.phase.trim() : 'approval';
  const status = typeof payload?.status === 'string' && payload.status.trim() ? ` ${payload.status.trim()}` : '';
  const title = typeof payload?.title === 'string' && payload.title.trim() ? payload.title.trim() : 'approval event';
  parts.push(`${phase}${status}: ${title}`);
  pushLine(parts, 'kind', payload?.kind);
  pushLine(parts, 'command', payload?.command);
  pushLine(parts, 'host', payload?.host);
  pushLine(parts, 'reason', payload?.reason);
  pushLine(parts, 'scope', payload?.scope);
  pushLine(parts, 'message', payload?.message);
  pushLine(parts, 'itemId', payload?.itemId);
  pushLine(parts, 'toolCallId', payload?.toolCallId);
  pushLine(parts, 'approvalId', payload?.approvalId);
  pushLine(parts, 'approvalSlug', payload?.approvalSlug);
  return parts.join('\n');
}

export function formatChatuCommandOutputPayload(payload: any): string {
  const output = typeof payload?.output === 'string' ? payload.output : '';
  if (output.length > 0 && payload?.phase === 'delta') return output;
  const hasOnlyOutput =
    output.length > 0 &&
    !payload?.status &&
    payload?.exitCode === undefined &&
    payload?.durationMs === undefined &&
    !payload?.cwd &&
    !payload?.title &&
    !payload?.name;
  if (hasOnlyOutput) return output;

  const parts: string[] = [];
  const phase = typeof payload?.phase === 'string' && payload.phase.trim() ? payload.phase.trim() : 'command';
  const title =
    (typeof payload?.title === 'string' && payload.title.trim()) ||
    (typeof payload?.name === 'string' && payload.name.trim()) ||
    'command output';
  parts.push(`${phase}: ${title}`);
  pushLine(parts, 'status', payload?.status);
  if (payload?.exitCode === null) {
    parts.push('exitCode: null');
  } else {
    pushLine(parts, 'exitCode', payload?.exitCode);
  }
  pushLine(parts, 'durationMs', payload?.durationMs);
  pushLine(parts, 'cwd', payload?.cwd);
  pushLine(parts, 'itemId', payload?.itemId);
  pushLine(parts, 'toolCallId', payload?.toolCallId);
  if (output) parts.push(output);
  return parts.join('\n');
}

export function formatChatuPatchSummaryPayload(payload: any): string {
  const parts: string[] = [];
  const phase = typeof payload?.phase === 'string' && payload.phase.trim() ? payload.phase.trim() : 'patch';
  const title =
    (typeof payload?.title === 'string' && payload.title.trim()) ||
    (typeof payload?.name === 'string' && payload.name.trim()) ||
    'patch summary';
  parts.push(`${phase}: ${title}`);
  pushLine(parts, 'summary', payload?.summary);
  const added = formatStringArray('added', payload?.added);
  const modified = formatStringArray('modified', payload?.modified);
  const deleted = formatStringArray('deleted', payload?.deleted);
  if (added) parts.push(added);
  if (modified) parts.push(modified);
  if (deleted) parts.push(deleted);
  pushLine(parts, 'itemId', payload?.itemId);
  pushLine(parts, 'toolCallId', payload?.toolCallId);
  return parts.join('\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// Plugin Entry Point
// ─────────────────────────────────────────────────────────────────────────────

export default function (api: OpenClawPluginApi) {
  // ── Config helpers ──────────────────────────────────────────────────────────

  api.logger.info('[chatu] Initializing channel plugin');

  // ── T015 Plugin-Channel Realtime: per-account outbound message caches ───────
  /** Stores failed AI replies for retry on reconnect. One per account. */
  const accountCaches = new Map<string, MessageCache>();

  /**
   * Bridges before_message_write → deliver callback so both relay and direct
   * delivery paths carry the same dedupId (the OpenClaw internal message ID).
   * Key: sessionKey, Value: OpenClaw msg.id
   */
  const pendingRelayIds = new Map<string, string>();

  function getAccountCache(accountId: string): MessageCache {
    if (!accountCaches.has(accountId)) {
      accountCaches.set(
        accountId,
        new MessageCache({
          logger: api.logger,
          maxCapacity: process.env.CHATU_CACHE_MAX ? parseInt(process.env.CHATU_CACHE_MAX, 10) : 1000,
          filePath: process.env.CHATU_CACHE_FILE
            ? `${process.env.CHATU_CACHE_FILE}.${accountId}.json`
            : undefined,
        }),
      );
    }
    return accountCaches.get(accountId)!;
  }


  // ── Config helpers ──────────────────────────────────────────────────────────

  /** Resolve per-account config, falling back to channel-level then plugin-level. */
  function getAccountConfig(accountId?: string | null) {
    const pluginCfg: Record<string, any> = api.config?.plugins?.entries?.chatu?.config ?? {};
    const channelCfg: Record<string, any> = api.config?.channels?.chatu ?? {};
    const accounts: Record<string, any> = channelCfg.accounts ?? {};
    const acctCfg: Record<string, any> =
      accountId && accounts[accountId] ? accounts[accountId] : {};

    return {
      apiUrl:     acctCfg.apiUrl     ?? channelCfg.apiUrl     ?? pluginCfg.apiUrl     ?? '',
      channelId:  acctCfg.channelId  ?? channelCfg.channelId  ?? pluginCfg.channelId  ?? '',
      secret:     acctCfg.secret     ?? channelCfg.secret     ?? pluginCfg.secret     ?? '',
      accessToken:acctCfg.accessToken?? channelCfg.accessToken?? pluginCfg.accessToken?? '',
      timeout:    acctCfg.timeout    ?? channelCfg.timeout    ?? pluginCfg.timeout    ?? DEFAULT_TIMEOUT_MS,
      streaming:  acctCfg.streaming  ?? channelCfg.streaming  ?? pluginCfg.streaming  ?? false,
      streamThrottleMs:
        acctCfg.streamThrottleMs ?? channelCfg.streamThrottleMs ?? pluginCfg.streamThrottleMs ?? DEFAULT_STREAM_THROTTLE_MS,
    };
  }

  // ── HTTP helpers ─────────────────────────────────────────────────────────────

  async function timedFetch(
    url: string,
    init: RequestInit,
    timeoutMs: number,
  ): Promise<Response> {
    const ctrl = new AbortController();
    const id = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      return await fetch(url, { ...init, signal: ctrl.signal });
    } finally {
      clearTimeout(id);
    }
  }

  // ── Lifecycle: register + connect ─────────────────────────────────────────────

  /**
   * T023 Plugin-Channel Realtime: If CHATU_KEY and CHATU_URL env vars are set,
   * call POST /api/channel/quick-register to obtain credentials automatically.
   * This runs BEFORE registerAndConnect so WS setup (T012) can use the credentials.
   * Skipped if channelId + accessToken are already configured.
   */
  async function quickRegisterIfNeeded(accountId?: string | null): Promise<void> {
    const key = process.env.CHATU_KEY;
    const apiUrl = process.env.CHATU_URL ?? process.env.CHATU_API_URL;
    if (!key || !apiUrl) return;

    // If already have credentials, skip
    const cfg = getAccountConfig(accountId);
    if (cfg.channelId && cfg.accessToken) return;

    try {
      const resp = await timedFetch(
        `${apiUrl}/api/channel/quick-register`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ key, url: apiUrl }),
        },
        DEFAULT_TIMEOUT_MS,
      );

      if (resp.ok) {
        const data = await resp.json();
        const channelId: string | undefined = data?.data?.channelId;
        const accessToken: string | undefined = data?.data?.accessToken;

        if (channelId && accessToken) {
          const base = accountId
            ? `channels.chatu.accounts.${accountId}`
            : 'channels.chatu';
          try {
            await (api as any).config?.set?.(`${base}.channelId`, channelId);
            await (api as any).config?.set?.(`${base}.accessToken`, accessToken);
            await (api as any).config?.set?.(`${base}.apiUrl`, apiUrl);
          } catch (_) { /* config persistence optional */ }
          api.logger.info(
            `[chatu] Quick-registered via CHATU_KEY (channelId=${channelId}, account=${accountId ?? 'default'})`,
          );
        }
      } else {
        api.logger.warn(
          `[chatu] Quick-register returned HTTP ${resp.status} — check CHATU_KEY/CHATU_URL`,
        );
      }
    } catch (err) {
      api.logger.warn(`[chatu] Quick-register failed: ${String(err)}`);
    }
  }

  async function registerAndConnect(accountId?: string | null): Promise<void> {
    const cfg = getAccountConfig(accountId);
    if (!cfg.apiUrl) return;

    // Connect directly using accessToken (secret-based registration removed;
    // credentials are obtained via quick-register or manual config).
    const refreshed = getAccountConfig(accountId);
    if (refreshed.accessToken && refreshed.channelId) {
      try {
        const resp = await timedFetch(
          `${refreshed.apiUrl}/api/channel/connect`,
          {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'x-access-token': refreshed.accessToken,
            },
            body: JSON.stringify({
              channelId: refreshed.channelId,
              pluginVersion: pkg.version,
              workingDir: os.homedir(),
            }),
          },
          refreshed.timeout,
        );
        if (resp.ok) {
          api.logger.info(`[chatu] Channel connected (channelId=${refreshed.channelId}, v${pkg.version}, workingDir=${os.homedir()})`);
        }
      } catch (err) {
        api.logger.warn(`[chatu] Connect request failed: ${String(err)}`);
      }
    }
  }

  async function disconnectAccount(accountId?: string | null): Promise<void> {
    const cfg = getAccountConfig(accountId);
    if (!cfg.apiUrl || !cfg.accessToken || !cfg.channelId) return;
    try {
      await timedFetch(
        `${cfg.apiUrl}/api/channel/disconnect`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'x-access-token': cfg.accessToken,
          },
          body: JSON.stringify({ channelId: cfg.channelId }),
        },
        cfg.timeout,
      );
    } catch (_) { /* best-effort */ }
  }

  // ── Inbound: deliver AI reply back to service ────────────────────────────────

  async function deliverOutbound(params: {
    text: string;
    target: string;
    accountId?: string | null;
    replyTo?: string | null;
    mediaUrl?: string;
    mediaType?: string;
    messageType?: string;
    metadata?: Record<string, unknown>;
    raw?: unknown;
  }): Promise<{ ok: boolean; messageId?: string; error?: string }> {
    const cfg = getAccountConfig(params.accountId);
    if (!cfg.apiUrl || !cfg.accessToken) {
      return { ok: false, error: 'Missing apiUrl or accessToken' };
    }

    const messageId = `msg_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const payload: Record<string, unknown> = {
      messageId,
      target: { type: 'user', id: params.target },
      content: { text: params.text, format: 'plain' },
      timestamp: Date.now(),
    };
    if (params.replyTo) payload.replyTo = { id: params.replyTo };
    if (params.mediaUrl) {
      payload.media = [{ type: params.mediaType ?? 'file', url: params.mediaUrl }];
    }
    if (params.messageType) payload.messageType = params.messageType;
    if (params.metadata) payload.metadata = params.metadata;
    // Phase 11 T049: always stamp role:'ai' so the service can persist the correct author role
    payload.role = 'ai';
    if (params.raw !== undefined) payload.raw = params.raw;

    try {
      const resp = await timedFetch(
        `${cfg.apiUrl}/api/channel/messages`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Channel-Token': cfg.accessToken,
            'X-Channel-ID': cfg.channelId,
          },
          body: JSON.stringify(payload),
        },
        cfg.timeout,
      );
      if (!resp.ok) {
        const errorText = await resp.text();
        return { ok: false, error: `HTTP ${resp.status}: ${errorText}` };
      }
      const result = await resp.json();
      return { ok: true, messageId: result.messageId ?? messageId };
    } catch (err: any) {
      return { ok: false, error: String(err?.message ?? err) };
    }
  }

  // ── Streaming relay helpers (T042) ────────────────────────────────────────

  /**
   * Relay a single streaming chunk to the WebHub API.
   * Called by the outbound.sendStreamChunk handler when OpenClaw AI streams.
   */
  async function deliverStreamChunk(params: {
    messageId: string;
    seq: number;
    delta: string;
    type?: ChatuStreamChunkType;
    accountId?: string | null;
  }): Promise<{ ok: boolean; error?: string }> {
    const cfg = getAccountConfig(params.accountId);
    if (!cfg.apiUrl || !cfg.accessToken) {
      return { ok: false, error: 'Missing apiUrl or accessToken' };
    }
    try {
      const resp = await timedFetch(
        `${cfg.apiUrl}/api/channel/stream/chunk`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${cfg.accessToken}`,
          },
          body: JSON.stringify({ messageId: params.messageId, seq: params.seq, delta: params.delta, type: params.type }),
        },
        cfg.timeout,
      );
      if (!resp.ok) {
        const errorText = await resp.text();
        return { ok: false, error: `HTTP ${resp.status}: ${errorText}` };
      }
      return { ok: true };
    } catch (err: any) {
      return { ok: false, error: String(err?.message ?? err) };
    }
  }

  /**
   * Signal streaming completion to the WebHub API.
   * Called by the outbound.sendStreamDone handler when OpenClaw AI finishes.
   */
  async function deliverStreamDone(params: {
    messageId: string;
    totalSeq: number;
    accountId?: string | null;
  }): Promise<{ ok: boolean; error?: string }> {
    const cfg = getAccountConfig(params.accountId);
    if (!cfg.apiUrl || !cfg.accessToken) {
      return { ok: false, error: 'Missing apiUrl or accessToken' };
    }
    try {
      const resp = await timedFetch(
        `${cfg.apiUrl}/api/channel/stream/done`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${cfg.accessToken}`,
          },
          body: JSON.stringify({ messageId: params.messageId, totalSeq: params.totalSeq }),
        },
        cfg.timeout,
      );
      if (!resp.ok) {
        const errorText = await resp.text();
        return { ok: false, error: `HTTP ${resp.status}: ${errorText}` };
      }
      return { ok: true };
    } catch (err: any) {
      return { ok: false, error: String(err?.message ?? err) };
    }
  }

  // ── T011 US3: Cross-channel relay helpers ────────────────────────────────────

  /**
   * Forward a message that arrived on another OpenClaw channel (e.g. TUI,
   * WhatsApp, Telegram) to this ChatU WebHub channel so the conversation
   * appears in the frontend with a cross-channel badge.
   *
   * Call this from any OpenClaw integration point that has access to the
   * per-channel message — for example from an OpenClaw `before_message_write`
   * hook (when it becomes available in the SDK), or from a custom relay script.
   *
   * @param params.sourceChannel  Originating channel id (e.g. 'tui', 'whatsapp')
   * @param params.direction      'inbound' (AI reply) or 'outbound' (user message)
   * @param params.senderName     Display name of the sender
   * @param params.content        Text content of the message
   * @param params.sessionKey     Session key in the originating channel
   * @param params.accountId      ChatU account id (defaults to 'default')
   */
  async function relayCrossChannelMessage(params: {
    sourceChannel: string;
    direction: 'inbound' | 'outbound';
    sender: { id?: string; name: string };
    content: string;
    sessionKey: string;
    accountId?: string | null;
    dedupId?: string;
    raw?: unknown;
  }): Promise<{ ok: boolean; id?: string; error?: string }> {
    const cfg = getAccountConfig(params.accountId);
    if (!cfg.apiUrl || !cfg.accessToken) {
      return { ok: false, error: 'Missing apiUrl or accessToken for cross-channel relay' };
    }

    try {
      const resp = await timedFetch(
        `${cfg.apiUrl}/api/channel/cross-channel-messages`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Access-Token': cfg.accessToken,
          },
          body: JSON.stringify({
            sourceChannel: params.sourceChannel,
            direction: params.direction,
            sender: params.sender,
            content: params.content,
            sessionKey: params.sessionKey,
            ...(params.dedupId ? { dedupId: params.dedupId } : {}),
            ...(params.raw !== undefined ? { raw: params.raw } : {}),
          }),
        },
        cfg.timeout,
      );

      if (!resp.ok) {
        const errorText = await resp.text();
        api.logger.warn(
          `[chatu] cross-channel relay failed (source=${params.sourceChannel}): HTTP ${resp.status} ${errorText}`,
        );
        return { ok: false, error: `HTTP ${resp.status}: ${errorText}` };
      }

      const result = await resp.json();
      api.logger.info(
        `[chatu] cross_channel_relay_ok (source=${params.sourceChannel}, id=${result.id}, direction=${params.direction})`,
      );
      return { ok: true, id: result.id };
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      api.logger.error(`[chatu] cross-channel relay error: ${message}`);
      return { ok: false, error: message };
    }
  }

  // ── Gateway: poll + dispatch inbound user messages ───────────────────────────

  /**
   * Dispatch a single user message from the web client to the OpenClaw AI
   * pipeline using the PluginRuntime API.
   */
  function cacheStreamFrame(accountId: string, frame: ChatuStreamFrame, reason?: string): void {
    const cfg = getAccountConfig(accountId);
    const cache = getAccountCache(accountId);
    const id = frame.kind === 'stream_chunk'
      ? `stream:${frame.messageId}:${frame.seq}`
      : `stream:${frame.messageId}:done`;
    if (cache.snapshot().some((msg) => msg.id === id)) return;
    cache.enqueue({
      id,
      channelId: cfg.channelId,
      content: frame,
      enqueuedAt: Date.now(),
      status: 'pending',
    });
    api.logger.warn(
      `[chatu] cached stream frame for retry (account=${accountId}, id=${id}${reason ? `, reason=${reason}` : ''})`,
    );
  }

  function createChatuDraftStream(params: { messageId: string; accountId: string }) {
    const cfg = getAccountConfig(params.accountId);
    return createChatuStreamRelay({
      messageId: params.messageId,
      accountId: params.accountId,
      throttleMs: cfg.streamThrottleMs,
      deliverStreamChunk,
      deliverStreamDone,
      cacheFrame: (frame, reason) => cacheStreamFrame(params.accountId, frame, reason),
    });
  }

  async function dispatchUserMessage(params: {
    id: string;
    content: string;
    sender: { id?: string; name?: string };
    timestamp?: number;
    accountId: string;
    cfg: any;
  }): Promise<void> {
    const { id, content, sender, timestamp, accountId, cfg } = params;
    const senderId = sender.id ?? 'user';
    const senderName = sender.name;

    if (!content?.trim()) return;

    const runtime = api.runtime;
    if (!runtime?.channel?.reply?.dispatchReplyWithBufferedBlockDispatcher) {
      api.logger.warn(`[chatu] api.runtime not available; cannot dispatch inbound message (id=${id})`);
      return;
    }

    const to = `chatu:${senderId}`;
    const fromLabel = senderName ? `${senderName} (${senderId})` : senderId;

    try {
      const route = runtime.channel.routing.resolveAgentRoute({
        cfg,
        channel: CHANNEL_ID,
        accountId,
        peer: { kind: 'direct' as const, id: senderId },
      });

      const ctxPayload = runtime.channel.reply.finalizeInboundContext({
        Body: content,
        BodyForAgent: content,
        RawBody: content,
        CommandBody: content,
        From: `chatu:${senderId}`,
        To: to,
        SessionKey: route.sessionKey,
        AccountId: route.accountId,
        ChatType: 'direct',
        ConversationLabel: fromLabel,
        SenderName: senderName ?? senderId,
        SenderId: senderId,
        Provider: CHANNEL_ID,
        Surface: CHANNEL_ID,
        MessageSid: id,
        Timestamp: timestamp ?? Date.now(),
        OriginatingChannel: CHANNEL_ID,
        OriginatingTo: to,
        WasMentioned: true,
        // Authorize slash-commands (messages starting with '/'); regular messages remain unauthorized.
        CommandAuthorized: content.trim().startsWith('/'),
      });

      api.logger.info(`[chatu] Dispatching user message to AI (id=${id}, sender=${senderId})`);

      const streamingEnabled = getAccountConfig(accountId).streaming;
      const streamMessageId = streamingEnabled ? crypto.randomUUID() : undefined;
      const draftStream = streamMessageId
        ? createChatuDraftStream({ messageId: streamMessageId, accountId })
        : null;
      let streamingFinalPayload: any | undefined;
      const streamingReplyOptions = draftStream ? {
        disableBlockStreaming: true,
        onPartialReply: (p: any) => {
          draftStream.updateText(p.text ?? '');
        },
        onReasoningStream: (p: any) => {
          draftStream.updateReasoning(p.text ?? '');
        },
        onAssistantMessageStart: () => {
          return draftStream.resetForNewMessage();
        },
        onToolStart: (p: any) => {
          const delta = formatChatuToolStartPayload(p);
          return draftStream.sendChunkDirect(delta, 'tool_start');
        },
        onItemEvent: (p: any) => {
          const delta = formatChatuToolItemPayload(p);
          return draftStream.sendChunkDirect(delta, 'tool_item');
        },
        onPlanUpdate: (p: any) => {
          const delta = formatChatuToolPlanPayload(p);
          return draftStream.sendChunkDirect(delta, 'tool_plan');
        },
        onApprovalEvent: (p: any) => {
          const delta = formatChatuToolApprovalPayload(p);
          return draftStream.sendChunkDirect(delta, 'tool_approval');
        },
        onCommandOutput: (p: any) => {
          const delta = formatChatuCommandOutputPayload(p);
          return draftStream.sendChunkedDirect(delta, 'tool_command_output');
        },
        onPatchSummary: (p: any) => {
          const delta = formatChatuPatchSummaryPayload(p);
          return draftStream.sendChunkDirect(delta, 'tool_patch_summary');
        },
      } as any : {};

      try {
        await runtime.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
          ctx: ctxPayload,
          cfg,
          dispatcherOptions: {
            deliver: async (payload: any, info?: { kind?: string }) => {
              if (streamingEnabled) {
                if (info?.kind === 'tool' && draftStream) {
                  const toolResult = formatChatuToolResultPayload(payload);
                  if (toolResult) {
                    await draftStream.sendChunkedDirect(toolResult, 'tool_result');
                  }
                  return;
                }
                if (info?.kind === 'final' || !streamingFinalPayload) {
                  streamingFinalPayload = payload;
                }
                return;
              }
              const text: string = payload.text ?? '';
              if (!text) return;
              // Retrieve the dedupId stored by before_message_write for this session.
              const dedupId = pendingRelayIds.get(route.sessionKey as string);
              if (dedupId) pendingRelayIds.delete(route.sessionKey as string);
              const result = await deliverOutbound({
                text,
                target: senderId,
                accountId,
                replyTo: payload.replyToId ?? id,
                metadata: dedupId ? { dedupId } : undefined,
                raw: payload,
              });
              if (!result.ok) {
                api.logger.error(`[chatu] Failed to deliver AI reply (target=${senderId}): ${result.error}`);
                // T015 Plugin-Channel Realtime: cache failed delivery for retry on reconnect
                const cfg2 = getAccountConfig(accountId);
                const cache = getAccountCache(accountId);
                const cacheId = result.messageId ?? `retry_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
                cache.enqueue({
                  id: cacheId,
                  channelId: cfg2.channelId,
                  content: { text, target: senderId, replyTo: payload.replyToId ?? id },
                  enqueuedAt: Date.now(),
                  status: 'pending',
                });
              }
            },
            onError: (err: unknown, info: { kind: string }) => {
              api.logger.error(`[chatu] ${info.kind} reply failed: ${String(err)}`);
            },
          },
          replyOptions: streamingReplyOptions,
        });
      } finally {
        if (draftStream) {
          await draftStream.finalize();
        }
      }

      if (draftStream && !draftStream.hasTextFrames()) {
        const finalText: string = streamingFinalPayload?.text ?? '';
        if (finalText) {
          const dedupId = pendingRelayIds.get(route.sessionKey as string);
          if (dedupId) pendingRelayIds.delete(route.sessionKey as string);
          const result = await deliverOutbound({
            text: finalText,
            target: senderId,
            accountId,
            replyTo: streamingFinalPayload?.replyToId ?? id,
            metadata: dedupId ? { dedupId } : undefined,
            raw: streamingFinalPayload,
          });
          if (!result.ok) {
            const cfg2 = getAccountConfig(accountId);
            const cache = getAccountCache(accountId);
            const cacheId = result.messageId ?? `retry_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
            cache.enqueue({
              id: cacheId,
              channelId: cfg2.channelId,
              content: { text: finalText, target: senderId, replyTo: streamingFinalPayload?.replyToId ?? id },
              enqueuedAt: Date.now(),
              status: 'pending',
            });
          }
        }
      }
    } catch (err) {
      api.logger.error(`[chatu] Exception dispatching user message (id=${id}): ${String(err)}`);
    }
  }

  /**
   * Acknowledge that a message has been processed by the plugin.
   */
  async function ackMessage(
    apiUrl: string,
    accessToken: string,
    messageId: string,
    timeout: number,
  ): Promise<void> {
    try {
      await timedFetch(
        `${apiUrl}/api/channel/messages/${messageId}/ack`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Channel-Token': accessToken,
          },
        },
        timeout,
      );
    } catch (_) { /* best-effort */ }
  }

  // ── T012 display-sender-session: resolveSessionKey helper ──────────────────

  /**
   * Derive the OpenClaw sessionKey for a senderId using the same routing logic
   * as dispatchUserMessage. This is deterministic and requires no lookup table.
   */
  function resolveSessionKey(senderId: string, accountId: string, cfg: any): string {
    const runtime = api.runtime;
    const route = runtime.channel.routing.resolveAgentRoute({
      cfg,
      channel: CHANNEL_ID,
      accountId,
      peer: { kind: 'direct' as const, id: senderId },
    });
    return route.sessionKey as string;
  }

  // ── T011 display-sender-session: session command processor ─────────────────

  /**
   * Fetch and execute pending session commands for this channel.
   * Called at the end of each poll loop iteration.
   * Each command is acked (success or failure) before moving to the next.
   */
  async function processCommands(cfg: Omit<ChatuAccount, 'accountId'>, accountId: string): Promise<void> {
    if (!cfg.accessToken) return;

    const resp = await timedFetch(
      `${cfg.apiUrl}/api/channel/commands?channelId=${encodeURIComponent(cfg.channelId)}`,
      {
        method: 'GET',
        headers: {
          'X-Channel-Token': cfg.accessToken,
          'X-Channel-ID': cfg.channelId,
        },
      },
      cfg.timeout,
    );

    if (!resp.ok) return;

    const data = await resp.json();
    const commands: Array<{
      id: string;
      commandType: 'reset' | 'switch';
      senderId: string;
      payload?: { targetSessionKey?: string; reason?: string } | null;
    }> = data?.data?.commands ?? [];

    for (const cmd of commands) {
      let ackSuccess = false;
      let ackError: string | undefined;

      try {
        const freshCfg = api.config ?? {};
        const sessionKey = resolveSessionKey(cmd.senderId, accountId, freshCfg);

        if (cmd.commandType === 'reset') {
          // Resolve the sessions store directory and derive the transcript path
          const storePath = api.runtime.channel.session.resolveStorePath(
            (freshCfg as any)?.session?.store,
          );
          const transcriptPath = path.join(storePath, `${sessionKey}.jsonl`);
          try {
            await fs.unlink(transcriptPath);
            api.logger.info(`[chatu] Session reset: deleted transcript (key=${sessionKey})`);
          } catch (e: any) {
            if (e.code !== 'ENOENT') throw e;
            // ENOENT = already empty/non-existent, treat as success
          }
          ackSuccess = true;

        } else if (cmd.commandType === 'switch') {
          const targetSessionKey = cmd.payload?.targetSessionKey;
          if (!targetSessionKey) throw new Error('Missing targetSessionKey');

          const storePath = api.runtime.channel.session.resolveStorePath(
            (freshCfg as any)?.session?.store,
          );
          const currentPath = path.join(storePath, `${sessionKey}.jsonl`);
          const targetPath = path.join(storePath, `${targetSessionKey}.jsonl`);

          // Restore target session as the current session
          await fs.copyFile(targetPath, currentPath);
          api.logger.info(`[chatu] Session switched to ${targetSessionKey} (sender=${cmd.senderId})`);
          ackSuccess = true;
        }
      } catch (e: unknown) {
        ackError = String(e);
        api.logger.error(`[chatu] Command ${cmd.id} (${cmd.commandType}) failed: ${ackError}`);
      }

      // Ack regardless of outcome
      try {
        await timedFetch(
          `${cfg.apiUrl}/api/channel/commands/${cmd.id}/ack`,
          {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'X-Channel-Token': cfg.accessToken,
            },
            body: JSON.stringify({
              success: ackSuccess,
              error: ackError,
              channelId: cfg.channelId,
            }),
          },
          cfg.timeout,
        );
      } catch (_) { /* best-effort */ }
    }
  }

  /**
   * Plugin-Channel Realtime (T012): WebSocket-based gateway loop.
   * Replaces HTTP polling. Connects to /api/channel/ws via WebSocketAdapter
   * and dispatches inbound messages to the OpenClaw AI pipeline.
   * Reconnects automatically with infinite exponential back-off (T009).
   *
   * Runs until `abortSignal` fires.
   */
  async function wsConnectionLoop(ctx: {
    accountId: string;
    abortSignal: AbortSignal;
    setStatus: (s: ChannelAccountSnapshot) => void;
    log?: ChannelLogSink;
  }): Promise<void> {
    const cfg = getAccountConfig(ctx.accountId);

    if (!cfg.apiUrl) {
      ctx.log?.error?.(`[${ctx.accountId}] chatu: missing apiUrl for WS connection`);
      return;
    }
    if (!cfg.accessToken || !cfg.channelId) {
      ctx.log?.error?.(`[${ctx.accountId}] chatu: missing accessToken/channelId for WS connection`);
      return;
    }

    // Convert HTTP URL to WebSocket URL scheme
    const wsBase = cfg.apiUrl
      .replace(/^https:\/\//, 'wss://')
      .replace(/^http:\/\//, 'ws://');

    const adapter = new WebSocketAdapter({
      channelId: cfg.channelId,
      accessToken: cfg.accessToken,
      webhubUrl: `${wsBase}/api/channel/ws`,
    });

    // Register inbound message handler — dispatches user messages to AI
    adapter.onMessage(async (msg: InboundMessage) => {
      const text = msg.content?.text?.trim() ?? '';
      if (!text) return;

      const freshCfg = api.config ?? {};

      // Phase 11 T048 (fixed): role:agent frames come from the human operator via the
      // webhub frontend.  api.dispatch() does not exist in the OpenClaw plugin SDK;
      // instead we re-use dispatchUserMessage so the agent message appears in OpenClaw's
      // conversation context (sender = 'webhub-agent').  OpenClaw AI may reply; if it does,
      // the reply is delivered via deliverOutbound → /api/channel/messages → frontend.
      const senderId = (msg as any).role === 'agent'
        ? ((msg as any).sender?.id ?? 'webhub-agent')
        : msg.sender.id;
      const senderName = (msg as any).role === 'agent'
        ? ((msg as any).sender?.displayName ?? 'Agent')
        : msg.sender.displayName;

      await dispatchUserMessage({
        id: msg.id,
        content: text,
        sender: { id: senderId, name: senderName ?? undefined },
        timestamp: msg.timestamp,
        accountId: ctx.accountId,
        cfg: freshCfg,
      });
    });

    // Track connection status → surface to OpenClaw gateway
    adapter.onStatusChange((status, err) => {
      if (status === 'connected') {
        ctx.setStatus({ accountId: ctx.accountId, connected: true });
        ctx.log?.info?.(`[${ctx.accountId}] chatu: WebSocket connected`);
      } else if (status === 'disconnected') {
        ctx.setStatus({ accountId: ctx.accountId, connected: false });
      } else if (status === 'error') {
        ctx.setStatus({
          accountId: ctx.accountId,
          connected: false,
          lastError: err?.message ?? 'WS error',
        });
      }
    });

    // T015 Plugin-Channel Realtime: flush cached failed deliveries on reconnect
    adapter.onReconnected(async () => {
      const cache = getAccountCache(ctx.accountId);
      if (cache.pendingCount === 0) return;
      api.logger.info(
        `[chatu] Reconnected — flushing ${cache.pendingCount} cached messages (account=${ctx.accountId})`,
      );
      await cache.flush(async (cachedMsg) => {
        const streamFrame = cachedMsg.content as Partial<ChatuStreamFrame>;
        if (streamFrame.kind === 'stream_chunk') {
          const result = await deliverStreamChunk(streamFrame as Extract<ChatuStreamFrame, { kind: 'stream_chunk' }>);
          if (!result.ok) {
            throw new Error(result.error ?? 'Cached stream chunk delivery failed');
          }
          cache.ack(cachedMsg.id);
          return;
        }
        if (streamFrame.kind === 'stream_done') {
          const result = await deliverStreamDone(streamFrame as Extract<ChatuStreamFrame, { kind: 'stream_done' }>);
          if (!result.ok) {
            throw new Error(result.error ?? 'Cached stream done delivery failed');
          }
          cache.ack(cachedMsg.id);
          return;
        }

        const payload = cachedMsg.content as { text: string; target: string; replyTo?: string };
        const result = await deliverOutbound({
          text: payload.text ?? '',
          target: payload.target ?? '',
          accountId: ctx.accountId,
          replyTo: payload.replyTo,
        });
        if (!result.ok) {
          throw new Error(result.error ?? 'Cached delivery failed');
        }
        cache.ack(cachedMsg.id);
      });
    });

    ctx.setStatus({ accountId: ctx.accountId, connected: false });
    ctx.log?.info?.(`[${ctx.accountId}] chatu: starting WebSocket connection to ${wsBase}/api/channel/ws`);

    // Attempt initial connect (adapter auto-reconnects indefinitely on failure)
    try {
      await adapter.connect();
    } catch (err) {
      api.logger.warn(`[chatu] Initial WS connect failed (account=${ctx.accountId}): ${String(err)}`);
      // Adapter will keep retrying — proceed to wait for abort
    }

    // Hold until the gateway signals shutdown
    await new Promise<void>((resolve) => {
      if (ctx.abortSignal.aborted) { resolve(); return; }
      ctx.abortSignal.addEventListener('abort', () => resolve(), { once: true });
    });

    ctx.log?.info?.(`[${ctx.accountId}] chatu: WebSocket connection stopping`);
    await adapter.disconnect();
    ctx.setStatus({ accountId: ctx.accountId, connected: false });
  }

  /**
   * @deprecated Use wsConnectionLoop instead (Plugin-Channel Realtime T012).
   * Long-running poll loop for the gateway.
   * Polls the WebHub service for new user messages and dispatches them to OpenClaw AI.
   * Runs until `abortSignal` fires.
   */
  async function pollLoop(ctx: {
    accountId: string;
    abortSignal: AbortSignal;
    setStatus: (s: ChannelAccountSnapshot) => void;
    log?: ChannelLogSink;
  }): Promise<void> {
    const { accountId, abortSignal } = ctx;

    // Pre-flight check
    const initCfg = getAccountConfig(accountId);
    if (!initCfg.apiUrl) {
      ctx.log?.error?.(`[${accountId}] chatu: missing apiUrl for polling`);
      return;
    }

    let lastCursor = '';
    let consecutiveErrors = 0;
    const MAX_ERRORS = 10;
    // Track processed message IDs to handle same-millisecond createdAt duplicates
    const processedIds = new Set<string>();
    const MAX_PROCESSED_IDS = 500;

    ctx.setStatus({ accountId: ctx.accountId, connected: true });
    ctx.log?.info?.(`[${accountId}] chatu: polling started`);

    while (!abortSignal.aborted) {
      // Exponential back-off: 2s → 4s → 8s → … capped at 30s on consecutive errors
      const backoffMs = Math.min(POLL_INTERVAL_MS * Math.pow(2, consecutiveErrors), MAX_BACKOFF_MS);
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, backoffMs);
        abortSignal.addEventListener('abort', () => { clearTimeout(timer); resolve(); }, { once: true });
      });

      if (abortSignal.aborted) break;

      try {
        // Re-read config each iteration so a refreshed accessToken is picked up
        const cfg = getAccountConfig(accountId);
        if (!cfg.accessToken) {
          consecutiveErrors++;
          api.logger.warn(`[chatu] No accessToken yet (account=${accountId}), retrying...`);
          continue;
        }

        const url =
          `${cfg.apiUrl}/api/channel/messages/pending` +
          `?channelId=${encodeURIComponent(cfg.channelId)}` +
          `&after=${encodeURIComponent(lastCursor)}`;

        const resp = await timedFetch(
          url,
          {
            method: 'GET',
            headers: {
              'X-Channel-Token': cfg.accessToken,
              'X-Channel-ID': accountId,
            },
          },
          cfg.timeout,
        );

        if (!resp.ok) {
          consecutiveErrors++;
          if (consecutiveErrors >= MAX_ERRORS) {
            ctx.setStatus({ accountId: ctx.accountId, connected: false, lastError: `HTTP ${resp.status}` });
          }
          continue;
        }

        consecutiveErrors = 0;
        ctx.setStatus({ accountId: ctx.accountId, connected: true });

        const data = await resp.json();
        const messages: any[] = data?.data ?? [];

        for (const msg of messages) {
          // Advance ISO timestamp cursor so next poll fetches only newer messages
          if (msg.createdAt) lastCursor = msg.createdAt as string;

          // Skip messages already processed in-memory (handles same-ms duplicates)
          if (processedIds.has(msg.id)) continue;
          processedIds.add(msg.id);
          // Bound set growth
          if (processedIds.size > MAX_PROCESSED_IDS) {
            const first = processedIds.values().next().value;
            if (first !== undefined) processedIds.delete(first);
          }

          // Ack first (idempotency)
          await ackMessage(cfg.apiUrl, cfg.accessToken, msg.id, cfg.timeout);

          // T099: send typing indicator before dispatching to AI
          const typingChannelId = msg.channelId ?? cfg.channelId;
          if (typingChannelId) {
            timedFetch(
              `${cfg.apiUrl}/api/channel/typing`,
              {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Channel-Token': cfg.accessToken },
                body: JSON.stringify({ channelId: typingChannelId }),
              },
              3000,
            ).catch(() => { /* best-effort */ });
          }

          const freshCfg = api.config ?? {};
          await dispatchUserMessage({
            id: msg.id,
            content: msg.content ?? msg.text ?? '',
            sender: {
              id: (msg as any).sender?.id ?? 'user',
              name: (msg as any).sender?.name,
            },
            timestamp: msg.createdAt
              ? new Date(msg.createdAt).getTime()
              : Date.now(),
            accountId,
            cfg: freshCfg,
          });
        }

        // T011 display-sender-session: process pending session commands
        await processCommands(cfg, accountId).catch((e) => {
          api.logger.warn(`[chatu] processCommands error (account=${accountId}): ${String(e)}`);
        });
      } catch (err) {
        consecutiveErrors++;
        api.logger.warn(`[chatu] Poll failed (account=${accountId}, errors=${consecutiveErrors}): ${String(err)}`);
        if (consecutiveErrors >= MAX_ERRORS) {
          ctx.setStatus({ accountId: ctx.accountId, connected: false, lastError: String(err) });
        }
      }
    }

    ctx.setStatus({ accountId: ctx.accountId, connected: false });
    ctx.log?.info?.(`[${accountId}] chatu: polling stopped`);
  }

  // ── Channel Plugin Definition ────────────────────────────────────────────────

  const chatuChannel: ChannelPlugin<ChatuAccount> = {
    id: CHANNEL_ID,

    // ── Metadata ──────────────────────────────────────────────────────────────
    meta: {
      id: CHANNEL_ID,
      label: 'Chatu',
      selectionLabel: 'Chatu (HTTP/WebSocket)',
      docsPath: '/channels/chatu',
      blurb: 'Connect to any website via HTTP/WebSocket (WebHub service)',
      aliases: ['chatu', 'http-channel', 'webhub'],
    },

    // ── Capabilities ──────────────────────────────────────────────────────────
    capabilities: {
      chatTypes: ['direct', 'group'] as Array<'direct' | 'group'>,
      reply: true,
      edit: true,
      unsend: true,
      reactions: true,
      polls: false,
      media: true,
      threads: true,
      blockStreaming: false,
    },

    defaults: { queue: { debounceMs: 0 } },

    // ── Config Schema (UI hints) ───────────────────────────────────────────────
    configSchema: {
      schema: {
        type: 'object',
        properties: {
          apiUrl:      { type: 'string', description: 'WebHub service base URL' },
          channelId:   { type: 'string', description: 'Channel ID from WebHub' },
          secret:      { type: 'string', description: 'Channel secret (wh_secret_...)' },
          accessToken: { type: 'string', description: 'Access token' },
          timeout:     { type: 'number', description: 'Request timeout in ms' },
          streaming:   { type: 'boolean', description: 'Enable streaming mode (sends chunks via /api/channel/stream/chunk)' },
          streamThrottleMs: {
            type: 'number',
            description: 'Streaming text/reasoning coalescing delay in ms. Set 0 to send one chunk per OpenClaw partial event.',
          },
        },
      },
      uiHints: {
        apiUrl: {
          label: 'API URL',
          placeholder: 'https://your-webhub-service.example.com',
          help: 'Base URL of the Chatu WebHub service',
        },
        channelId: {
          label: 'Channel ID',
          placeholder: 'wh_ch_xxxxxx',
          help: 'Channel ID from the WebHub service',
        },
        secret: {
          label: 'Channel Secret',
          sensitive: true,
          placeholder: 'wh_secret_xxxxxxxxxx',
        },
        accessToken: {
          label: 'Access Token',
          sensitive: true,
          placeholder: 'wh_xxxxxxxxxxxxxxxx',
          advanced: true,
        },
        timeout: { label: 'Timeout (ms)', placeholder: '30000', advanced: true },
        streaming: { label: 'Streaming Mode', help: 'Stream AI responses chunk-by-chunk to the frontend', advanced: true },
        streamThrottleMs: {
          label: 'Stream Throttle (ms)',
          placeholder: '0',
          help: '0 sends a WebHub chunk for every OpenClaw partial text/reasoning event',
          advanced: true,
        },
      },
    },

    // ── Setup (CLI) ───────────────────────────────────────────────────────────
    setup: {
      applyAccountConfig: ({ cfg, accountId, input }: { cfg: OpenClawConfig; accountId: string; input: ChannelSetupInput }) => {
        const chatInput = input as ChatuSetupInput;
        const next = { ...cfg } as Record<string, any>;
        if (!next['channels']) next['channels'] = {};
        if (!next['channels'].chatu) next['channels'].chatu = {};
        if (!next['channels'].chatu.accounts) next['channels'].chatu.accounts = {};
        if (!next['channels'].chatu.accounts[accountId]) {
          next['channels'].chatu.accounts[accountId] = {};
        }
        const acct = next['channels'].chatu.accounts[accountId];
        if (chatInput.apiUrl)        acct.apiUrl      = chatInput.apiUrl;
        if (chatInput.channelId)     acct.channelId   = chatInput.channelId;
        if (chatInput.secret)        acct.secret      = chatInput.secret;
        if (input.accessToken)       acct.accessToken = input.accessToken;
        return next as OpenClawConfig;
      },
      validateInput: ({ input }: { cfg: OpenClawConfig; accountId: string; input: ChannelSetupInput }): string | null => {
        const chatInput = input as ChatuSetupInput;
        if (!chatInput.apiUrl)    return 'apiUrl is required';
        if (!chatInput.channelId) return 'channelId is required';
        if (!chatInput.secret && !input.accessToken)
          return 'Either secret or accessToken is required';
        return null;
      },
    },

    // ── Config ────────────────────────────────────────────────────────────────
    config: {
      listAccountIds: (cfg: OpenClawConfig): string[] => {
        const accounts = cfg?.channels?.chatu?.accounts ?? {};
        const ids = Object.keys(accounts);
        if (
          ids.length === 0 &&
          (cfg?.channels?.chatu?.apiUrl || cfg?.channels?.chatu?.channelId)
        ) {
          return ['default'];
        }
        return ids;
      },

      resolveAccount: (cfg: OpenClawConfig, accountId?: string | null): ChatuAccount => {
        const accounts   = cfg?.channels?.chatu?.accounts ?? {};
        const channelCfg = cfg?.channels?.chatu ?? {};
        const id         = accountId ?? 'default';
        const acct       = accounts[id] ?? {};
        return {
          accountId:   id,
          apiUrl:      acct.apiUrl      ?? channelCfg.apiUrl      ?? '',
          channelId:   acct.channelId   ?? channelCfg.channelId   ?? '',
          secret:      acct.secret      ?? channelCfg.secret,
          accessToken: acct.accessToken ?? channelCfg.accessToken,
          timeout:     acct.timeout     ?? channelCfg.timeout     ?? DEFAULT_TIMEOUT_MS,
          streaming:   acct.streaming   ?? channelCfg.streaming   ?? false,
          streamThrottleMs:
            acct.streamThrottleMs ?? channelCfg.streamThrottleMs ?? DEFAULT_STREAM_THROTTLE_MS,
        };
      },

      isConfigured: (account: ChatuAccount, _cfg: OpenClawConfig): boolean =>
        Boolean(
          account?.apiUrl &&
          account?.channelId &&
          (account?.accessToken || account?.secret),
        ),

      unconfiguredReason: (account: ChatuAccount, _cfg: OpenClawConfig): string => {
        if (!account?.apiUrl)    return 'apiUrl not configured';
        if (!account?.channelId) return 'channelId not configured';
        if (!account?.accessToken && !account?.secret)
          return 'accessToken or secret not configured';
        return 'Not configured';
      },

      isEnabled: (account: ChatuAccount, cfg: OpenClawConfig): boolean => {
        if (cfg?.channels?.chatu?.enabled === false) return false;
        return Boolean(account?.apiUrl);
      },

      disabledReason: (_account: ChatuAccount, cfg: OpenClawConfig): string => {
        if (cfg?.channels?.chatu?.enabled === false) return 'Channel disabled in config';
        return 'Not enabled';
      },

      describeAccount: (account: ChatuAccount, _cfg: OpenClawConfig): ChannelAccountSnapshot => ({
        accountId: account.accountId,
        name:      `Chatu (${account.channelId || account.accountId || 'unknown'})`,
        connected: Boolean(account.accessToken),
        baseUrl:   account.apiUrl || undefined,
      }),
    },

    // ── Pairing ───────────────────────────────────────────────────────────────
    pairing: {
      idLabel: 'Channel ID',
      normalizeAllowEntry: (entry: string) => entry.trim().toLowerCase(),
    },

    // ── Security ──────────────────────────────────────────────────────────────
    security: {
      resolveDmPolicy: () => null, // WebHub controls access
    },

    // ── Groups ────────────────────────────────────────────────────────────────
    groups: {
      resolveRequireMention: () => false,
    },

    // ── Streaming ─────────────────────────────────────────────────────────────
    streaming: {
      blockStreamingCoalesceDefaults: { minChars: 40, idleMs: 300 },
    },

    // ── Threading ─────────────────────────────────────────────────────────────
    threading: {
      resolveReplyToMode: () => 'first' as const,
      allowExplicitReplyTagsWhenOff: true,
    },

    // ── Messaging ─────────────────────────────────────────────────────────────
    messaging: {
      normalizeTarget: (raw: string) =>
        raw?.trim().replace(/^chatu:/i, '').toLowerCase() || undefined,
      targetResolver: {
        looksLikeId: (raw: string) => Boolean(raw?.trim()),
        hint: 'User ID or channel ID from the WebHub service',
      },
    },

    // ── Status ────────────────────────────────────────────────────────────────
    status: {
      probeAccount: async ({ account, timeoutMs }: { account: ChatuAccount; timeoutMs: number; cfg: OpenClawConfig }) => {
        const cfg = getAccountConfig(account?.accountId);
        if (!cfg.apiUrl || !cfg.accessToken) {
          return { ok: false, error: 'Not configured' };
        }
        try {
          const resp = await timedFetch(
            `${cfg.apiUrl}/api/channel/status`,
            {
              headers: {
                'x-access-token': cfg.accessToken,
                'X-Channel-ID': account?.accountId ?? cfg.channelId,
              },
            },
            Math.min(timeoutMs, 5000),
          );
          if (resp.ok) {
            const data = await resp.json();
            return { ok: true, status: data?.data?.status ?? 'unknown' };
          }
          return { ok: false, error: `HTTP ${resp.status}` };
        } catch (err: any) {
          return { ok: false, error: String(err?.message ?? err) };
        }
      },

      buildAccountSnapshot: ({ account, probe }: { account: ChatuAccount; cfg: OpenClawConfig; probe?: unknown }): ChannelAccountSnapshot => {
        const p = probe as { ok?: boolean; error?: string; status?: string } | undefined;
        return {
          accountId: account.accountId,
          connected: p?.ok === true,
          lastError: p?.ok ? null : (p?.error ?? null),
          baseUrl:   account.apiUrl || undefined,
          name:      `Chatu (${account.channelId || account.accountId})`,
        };
      },
    },

    // ── Heartbeat ─────────────────────────────────────────────────────────────
    heartbeat: {
      checkReady: async ({ accountId }: { cfg: OpenClawConfig; accountId?: string | null; deps?: unknown }) => {
        const aid = accountId ?? 'default';
        const cfg = getAccountConfig(aid);
        if (!cfg.apiUrl)      return { ok: false, reason: 'apiUrl not configured' };
        if (!cfg.accessToken) return { ok: false, reason: 'accessToken not configured' };
        try {
          const resp = await timedFetch(`${cfg.apiUrl}/health`, {}, 5000);
          if (resp.ok) return { ok: true, reason: 'Service reachable' };
          return { ok: false, reason: `Service returned HTTP ${resp.status}` };
        } catch (err: any) {
          return {
            ok: false,
            reason: `Cannot reach service: ${String(err?.message ?? err)}`,
          };
        }
      },
    },

    // ── Gateway (long-running per-account connection) ─────────────────────────
    gateway: {
      startAccount: async (ctx: ChannelGatewayContext<ChatuAccount>): Promise<void> => {
        api.logger.info(`[chatu] WebHub channel plugin v${pkg.version} starting`);
        // T023: quick-register via env vars if no credentials configured
        await quickRegisterIfNeeded(ctx.accountId);
        await registerAndConnect(ctx.accountId);
        // Plugin-Channel Realtime (T012): use WebSocket instead of HTTP polling
        await wsConnectionLoop({
          accountId:   ctx.accountId,
          abortSignal: ctx.abortSignal,
          setStatus:   ctx.setStatus,
          log:         ctx.log,
        });
      },

      stopAccount: async (ctx: ChannelGatewayContext<ChatuAccount>): Promise<void> => {
        await disconnectAccount(ctx.accountId);
      },

      logoutAccount: async (ctx: ChannelLogoutContext<ChatuAccount>) => {
        const { accountId } = ctx;
        const cfgKey =
          accountId === 'default'
            ? 'channels.chatu.accessToken'
            : `channels.chatu.accounts.${accountId}.accessToken`;
        try { await (api as any).config?.set?.(cfgKey, ''); } catch (_) { /* ok */ }
        await disconnectAccount(accountId);
        return { cleared: true, loggedOut: true };
      },
    },

    // ── Outbound ──────────────────────────────────────────────────────────────
    outbound: {
      deliveryMode: 'direct' as const,
      textChunkLimit: DEFAULT_CHUNK_LIMIT,

      resolveTarget: (params) => {
        const raw = params?.to ?? params?.accountId ?? 'default';
        const normalized = String(raw).trim().replace(/^chatu:/i, '');
        if (!normalized) {
          return { ok: false as const, error: new Error('Empty target') };
        }
        return { ok: true as const, to: normalized };
      },

      sendText: async (ctx) => {
        const { to, text, accountId, replyToId, silent } = ctx;
        if (silent) return { channel: CHANNEL_ID, messageId: 'silent' };

        const result = await deliverOutbound({ text, target: to, accountId, replyTo: replyToId, raw: ctx });

        if (!result.ok) {
          api.logger.error(`[chatu] Failed to send text (to=${to}): ${result.error}`);
          throw new Error(result.error ?? 'sendText failed');
        }
        api.logger.info(`[chatu] Text sent (to=${to}, messageId=${result.messageId})`);
        return { channel: CHANNEL_ID, messageId: result.messageId ?? '' };
      },

      sendMedia: async (ctx) => {
        const { to, mediaUrl, text, accountId, replyToId } = ctx;
        // Infer mediaType from URL extension since ChannelOutboundContext has no mediaType field
        const inferMediaType = (url?: string): string => {
          if (!url) return 'file';
          const ext = url.split('?')[0].split('.').pop()?.toLowerCase() ?? '';
          if (['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'svg', 'ico'].includes(ext)) return 'image';
          if (['mp4', 'webm', 'mov', 'avi', 'mkv'].includes(ext)) return 'video';
          if (['mp3', 'wav', 'aac', 'flac', 'm4a'].includes(ext)) return 'audio';
          return 'file';
        };
        const result = await deliverOutbound({
          text: text ?? '',
          target: to,
          accountId,
          replyTo: replyToId,
          mediaUrl,
          mediaType: inferMediaType(mediaUrl),
          raw: ctx,
        });
        if (!result.ok) {
          api.logger.error(`[chatu] Failed to send media (to=${to}): ${result.error}`);
          throw new Error(result.error ?? 'sendMedia failed');
        }
        return { channel: CHANNEL_ID, messageId: result.messageId ?? '' };
      },

      // T098: send a rich payload (richCard, structured content)
      sendPayload: async (ctx: any) => {
        const { to, accountId, replyToId, messageType, metadata, text } = ctx;
        const result = await deliverOutbound({
          text: text ?? '',
          target: to,
          accountId,
          replyTo: replyToId,
          messageType,
          metadata,
          raw: ctx,
        });
        if (!result.ok) {
          api.logger.error(`[chatu] Failed to send payload (to=${to}): ${result.error}`);
          throw new Error(result.error ?? 'sendPayload failed');
        }
        return { channel: CHANNEL_ID, messageId: result.messageId ?? '' };
      },

      // T098: send a poll message
      sendPoll: async (ctx: any) => {
        const { to, accountId, replyToId, question, options, multiple } = ctx;
        const result = await deliverOutbound({
          text: question ?? 'Poll',
          target: to,
          accountId,
          replyTo: replyToId,
          messageType: 'poll',
          metadata: { poll: { question, options, multiple: multiple ?? false } },
          raw: ctx,
        });
        if (!result.ok) {
          api.logger.error(`[chatu] Failed to send poll (to=${to}): ${result.error}`);
          throw new Error(result.error ?? 'sendPoll failed');
        }
        return { channel: CHANNEL_ID, messageId: result.messageId ?? '' };
      },

    },
  };

  // ── Register channel with OpenClaw ─────────────────────────────────────────
  api.registerChannel({ plugin: chatuChannel });

  // ── T011 US3: Cross-channel relay via before_message_write hook ─────────────
  //
  // Every time OpenClaw writes a message to any session transcript, this hook
  // fires synchronously. We relay messages from channels OTHER than ChatU so
  // they show up in the ChatU frontend with a cross-channel badge.
  //
  // The hook MUST be synchronous. Async relay is fired-and-forgotten (.catch).
  api.on('before_message_write', (event, ctx) => {
    const sessionKey = ctx.sessionKey ?? '';

    // Skip ChatU's own channel sessions to prevent relay loops.
    // ChatU session keys always contain the CHANNEL_ID token 'chatu'.
    if (!sessionKey || sessionKey.includes('chatu')) return;

    const msg = event.message as any;
    const role: string = msg?.role ?? '';

    // Only relay user (outbound) and assistant (inbound) messages; skip tool/system.
    if (role !== 'user' && role !== 'assistant') return;

    const direction: 'inbound' | 'outbound' = role === 'assistant' ? 'inbound' : 'outbound';

    // Extract plain-text content from the AgentMessage (string or content-block array).
    let content = '';
    if (typeof msg.content === 'string') {
      content = msg.content;
    } else if (Array.isArray(msg.content)) {
      content = (msg.content as any[])
        .filter((b) => b?.type === 'text')
        .map((b) => b.text ?? '')
        .join('\n');
    }

    // Strip OpenClaw system metadata prefix and extract embedded metadata.
    // Pattern: "Conversation info (untrusted metadata): ```json\n{...}\n``` [date] actual_message"
    const metaPrefixMatch = content.match(
      /^Conversation info \(untrusted metadata\):\s*```(?:json)?\s*([\s\S]*?)```\s*(?:\[[^\]]*\])?\s*/,
    );
    if (metaPrefixMatch) {
      // Parse the embedded metadata to detect the sender channel.
      try {
        const embeddedMeta = JSON.parse(metaPrefixMatch[1].trim());
        // If the message originated from our own webhub frontend, skip relay to avoid duplicates.
        // OpenClaw injects sender_id="webhub" for messages forwarded from the chatu channel plugin.
        const embeddedSender: string = embeddedMeta?.sender_id ?? embeddedMeta?.sender ?? '';
        if (embeddedSender === 'webhub' || embeddedSender.startsWith('chatu')) return;
      } catch { /* ignore parse errors */ }
      // Strip the whole prefix regardless of parse success.
      content = content.replace(
        /^Conversation info \(untrusted metadata\):[\s\S]*?```[\s\S]*?```\s*(?:\[[^\]]*\])?\s*/,
        '',
      ).trim();
    }

    if (!content.trim()) return; // skip empty or tool-only messages

    // Derive source channel from session key.
    // Session key format: "{agentId}:{channel}:{peerId}" (approx.)
    // 'main' channel = TUI / CLI direct mode → label as 'tui'.
    const parts = sessionKey.split(':');
    const channelPart = parts[1] || parts[0] || 'tui';
    const rawSource = channelPart === 'main' ? 'tui' : channelPart;
    // Sanitize to match backend /^[a-z0-9_-]{1,64}$/ validation.
    const sourceChannel =
      rawSource
        .replace(/[^a-z0-9_-]/g, '-')
        .replace(/^-+|-+$/g, '')
        .slice(0, 64) || 'tui';

    const senderName = direction === 'inbound' ? 'OpenClaw' : sourceChannel;

    // Store the OpenClaw message ID so the deliver callback (deliverOutbound path)
    // can retrieve and attach it as dedupId, making both write paths carry the
    // same identifier for reliable ID-based dedup on the backend.
    const ocMsgId: string = (msg as any).id ?? '';
    if (ocMsgId) pendingRelayIds.set(sessionKey, ocMsgId);

    // Fire-and-forget with a short delay so that the direct deliverOutbound path
    // (which calls POST /api/channel/messages) has time to complete first.
    const RELAY_DEDUP_DELAY_MS = 500;
    setTimeout(() => {
      relayCrossChannelMessage({
        sourceChannel,
        direction,
        sender: { name: senderName },
        content: content.trim(),
        sessionKey,
        accountId: null,
        dedupId: ocMsgId || undefined,
        raw: msg,
      }).catch((err: unknown) => {
        api.logger.warn(
          `[chatu] before_message_write relay failed (source=${sourceChannel}, dir=${direction}): ${String(err)}`,
        );
      });
    }, RELAY_DEDUP_DELAY_MS);

    // Return undefined → don't block the message write.
  });

  api.logger.info('[chatu] Channel plugin loaded');

  // Return plugin lifecycle
  return {
    name: 'chatu-channel',
    async dispose() {
      api.logger.info('[chatu] Disposing channel plugin');
      await disconnectAccount();
    },
  };
}

// ── Testable utility exports ────────────────────────────────────────────────

/**
 * Computes the exponential back-off wait time in milliseconds.
 * On each consecutive error the wait doubles starting from baseMs, capped at maxMs.
 *
 * consecutiveErrors=0 → baseMs (normal interval, no back-off)
 * consecutiveErrors=1 → baseMs * 2
 * consecutiveErrors=2 → baseMs * 4
 * ...
 *
 * @param consecutiveErrors - Number of consecutive failures so far
 * @param baseMs            - Base interval in milliseconds (default 2000)
 * @param maxMs             - Maximum allowed wait in milliseconds (default 30000)
 */
export function computeBackoffMs(
  consecutiveErrors: number,
  baseMs: number = POLL_INTERVAL_MS,
  maxMs: number = MAX_BACKOFF_MS,
): number {
  return Math.min(baseMs * Math.pow(2, consecutiveErrors), maxMs);
}

/**
 * T011 US3 testable export: forward a cross-channel message to the ChatU WebHub
 * service so it appears in the frontend with a source-channel badge.
 *
 * Can be called from OpenClaw pipeline hooks (e.g. `before_message_write`) or
 * from standalone relay scripts that have access to the channel credentials.
 *
 * @param apiUrl         - WebHub service base URL
 * @param accessToken    - Channel access token (`X-Access-Token`)
 * @param sourceChannel  - Originating channel id  (e.g. 'tui', 'whatsapp')
 * @param direction      - 'inbound' (AI reply) or 'outbound' (user message)
 * @param sender         - Sender object: name required, id optional (cross-channel may lack user ID)
 * @param content        - Text content of the message
 * @param sessionKey     - Session key in the originating channel
 * @param timeoutMs      - Fetch timeout in milliseconds (default 30 s)
 */
export async function relayCrossChannelMessage(
  apiUrl: string,
  accessToken: string,
  sourceChannel: string,
  direction: 'inbound' | 'outbound',
  sender: { id?: string; name: string },
  content: string,
  sessionKey: string,
  timeoutMs: number = 30_000,
): Promise<{ ok: boolean; id?: string; error?: string }> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(`${apiUrl}/api/channel/cross-channel-messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Access-Token': accessToken,
      },
      body: JSON.stringify({ sourceChannel, direction, sender, content, sessionKey }),
      signal: ctrl.signal,
    });
    clearTimeout(timer);
    if (!resp.ok) {
      const errorText = await resp.text();
      return { ok: false, error: `HTTP ${resp.status}: ${errorText}` };
    }
    const result = await resp.json();
    return { ok: true, id: result.id };
  } catch (err: unknown) {
    clearTimeout(timer);
    return { ok: false, error: err instanceof Error ? err.message : String(err) };
  }
}

/**
 * T042 testable export: relay a streaming AI chunk to the WebHub API.
 *
 * @param apiUrl      - WebHub service base URL
 * @param accessToken - Channel access token (Bearer)
 * @param messageId   - Unique ID for the streaming message
 * @param seq         - 0-based sequential chunk index
 * @param delta       - Text delta for this chunk
 * @param timeoutMs   - Fetch timeout in milliseconds
 */
export async function relayStreamChunk(
  apiUrl: string,
  accessToken: string,
  messageId: string,
  seq: number,
  delta: string,
  timeoutMs: number = 30_000,
): Promise<{ ok: boolean; error?: string }> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(`${apiUrl}/api/channel/stream/chunk`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify({ messageId, seq, delta }),
      signal: ctrl.signal,
    });
    clearTimeout(timer);
    if (!resp.ok) {
      const errorText = await resp.text();
      return { ok: false, error: `HTTP ${resp.status}: ${errorText}` };
    }
    return { ok: true };
  } catch (err: any) {
    clearTimeout(timer);
    return { ok: false, error: String(err?.message ?? err) };
  }
}

/**
 * T042 testable export: signal streaming completion to the WebHub API.
 *
 * @param apiUrl      - WebHub service base URL
 * @param accessToken - Channel access token (Bearer)
 * @param messageId   - Unique ID for the streaming message
 * @param totalSeq    - Total number of chunks sent
 * @param timeoutMs   - Fetch timeout in milliseconds
 */
export async function relayStreamDone(
  apiUrl: string,
  accessToken: string,
  messageId: string,
  totalSeq: number,
  timeoutMs: number = 30_000,
): Promise<{ ok: boolean; error?: string }> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(`${apiUrl}/api/channel/stream/done`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify({ messageId, totalSeq }),
      signal: ctrl.signal,
    });
    clearTimeout(timer);
    if (!resp.ok) {
      const errorText = await resp.text();
      return { ok: false, error: `HTTP ${resp.status}: ${errorText}` };
    }
    return { ok: true };
  } catch (err: any) {
    clearTimeout(timer);
    return { ok: false, error: String(err?.message ?? err) };
  }
}
