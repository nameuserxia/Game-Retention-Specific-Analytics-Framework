import { useState, useCallback } from 'react';
import type {
  AnalysisState,
  FieldMappingRequest,
  AnalysisConfig,
  SchemaDiscoveryResponse,
  ValidationResponse,
  AnalysisResponse,
  JsonKeyDiscoveryResponse,
} from '../types/api';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

function formatUnknown(value: unknown): string {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function formatApiDetail(detail: unknown, fallback: string): string {
  if (!detail) return fallback;
  if (typeof detail === 'string') return detail;

  if (Array.isArray(detail)) {
    const messages = detail
      .map(item => {
        if (typeof item === 'string') return item;
        if (item && typeof item === 'object' && 'msg' in item) {
          const record = item as { loc?: unknown[]; msg?: unknown };
          const loc = Array.isArray(record.loc) ? record.loc.join('.') : '';
          return `${loc ? `${loc}：` : ''}${formatUnknown(record.msg)}`;
        }
        return formatUnknown(item);
      })
      .filter(Boolean);
    return messages.join('；') || fallback;
  }

  return formatUnknown(detail);
}

async function readError(response: Response, fallback: string) {
  try {
    const error = await response.json();
    return formatApiDetail(error.detail, fallback);
  } catch {
    return fallback;
  }
}

function normalizeSchema(schema: SchemaDiscoveryResponse): SchemaDiscoveryResponse {
  const columns = (schema.columns || []).map(formatUnknown);
  const columnSet = new Set(columns);

  const suggestions = Object.fromEntries(
    Object.entries(schema.suggestions || {}).map(([field, values]) => [
      field,
      (Array.isArray(values) ? values : [])
        .map(formatUnknown)
        .filter(value => value && columnSet.has(value)),
    ]),
  );

  return {
    ...schema,
    columns,
    suggestions,
    file_name: formatUnknown(schema.file_name),
    column_infos: (schema.column_infos || []).map(column => ({
      ...column,
      name: formatUnknown(column.name),
      dtype: formatUnknown(column.dtype),
      sample_values: (column.sample_values || []).map(formatUnknown),
    })),
  };
}

export function useAnalysis() {
  const [state, setState] = useState<AnalysisState>({ step: 'idle' });
  const [isLoading, setIsLoading] = useState(false);

  const updateState = useCallback((updates: Partial<AnalysisState>) => {
    setState(prev => ({ ...prev, ...updates }));
  }, []);

  const clearError = useCallback(() => {
    setState(prev => ({ ...prev, error: undefined }));
  }, []);

  const setError = useCallback((error: string) => {
    setState(prev => ({ ...prev, error, step: prev.step === 'analyzing' ? 'upload' : prev.step }));
  }, []);

  const reset = useCallback(() => {
    setState({ step: 'idle' });
    setIsLoading(false);
  }, []);

  const uploadFile = useCallback(async (file: File): Promise<SchemaDiscoveryResponse> => {
    setIsLoading(true);
    clearError();

    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await fetch(`${API_BASE}/api/upload`, {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error(await readError(response, '上传失败，请检查文件格式和内容。'));
      }

      const rawSchema: SchemaDiscoveryResponse = await response.json();
      const schema = normalizeSchema(rawSchema);
      updateState({
        step: 'upload',
        sessionId: schema.session_id,
        schema,
        validation: undefined,
        result: undefined,
      });
      return schema;
    } catch (error) {
      const message = error instanceof Error ? error.message : '上传失败，请稍后重试。';
      setError(message);
      throw error;
    } finally {
      setIsLoading(false);
    }
  }, [clearError, setError, updateState]);

  const validateMapping = useCallback(async (
    mapping: FieldMappingRequest,
    forceProceed: boolean = false,
  ): Promise<ValidationResponse> => {
    if (!state.sessionId) {
      throw new Error('当前没有可用的上传会话，请重新上传文件。');
    }

    setIsLoading(true);
    clearError();

    try {
      const params = new URLSearchParams({
        session_id: state.sessionId,
        force_proceed: String(forceProceed),
      });
      const response = await fetch(`${API_BASE}/api/validate-mapping?${params}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(mapping),
      });

      if (!response.ok) {
        throw new Error(await readError(response, '字段预校验失败。'));
      }

      const validation: ValidationResponse = await response.json();
      updateState({
        step: 'mapping',
        mapping,
        validation,
      });
      return validation;
    } catch (error) {
      const message = error instanceof Error ? error.message : '字段预校验失败。';
      setError(message);
      throw error;
    } finally {
      setIsLoading(false);
    }
  }, [clearError, setError, state.sessionId, updateState]);

  const detectJsonKeys = useCallback(async (
    jsonParamsCol: string,
    sampleSize: number = 5000,
  ): Promise<JsonKeyDiscoveryResponse> => {
    if (!state.sessionId) {
      throw new Error('当前没有可用的上传会话，请重新上传文件。');
    }

    clearError();
    const params = new URLSearchParams({
      session_id: state.sessionId,
      json_params_col: jsonParamsCol,
      sample_size: String(sampleSize),
    });

    const response = await fetch(`${API_BASE}/api/detect-json-keys?${params}`);
    if (!response.ok) {
      throw new Error(await readError(response, 'JSON Key 探测失败。'));
    }
    return response.json();
  }, [clearError, state.sessionId]);

  /**
   * 常规分析（非流式），分析完成后一次性返回完整结果。
   */
  const runAnalysis = useCallback(async (
    mapping: FieldMappingRequest,
    config: AnalysisConfig,
    forceProceed: boolean = false,
  ): Promise<AnalysisResponse> => {
    if (!state.sessionId) {
      throw new Error('当前没有可用的上传会话，请重新上传文件。');
    }

    setIsLoading(true);
    clearError();
    updateState({ step: 'analyzing' });

    try {
      const params = new URLSearchParams({
        session_id: state.sessionId,
        force_proceed: String(forceProceed),
      });
      const { param_config, analysis_context, ai_enabled, ...analysisConfig } = config;
      const response = await fetch(`${API_BASE}/api/analyze?${params}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mapping,
          analysis_config: analysisConfig,
          param_config,
          analysis_context,
          ai_enabled: Boolean(ai_enabled),
        }),
      });

      if (!response.ok) {
        throw new Error(await readError(response, '分析失败，请检查日期窗口和字段映射。'));
      }

      const result: AnalysisResponse = await response.json();
      updateState({
        step: 'done',
        mapping,
        analysisConfig: config,
        result,
      });
      return result;
    } catch (error) {
      const message = error instanceof Error ? error.message : '分析失败，请检查日期窗口和字段映射。';
      setError(message);
      throw error;
    } finally {
      setIsLoading(false);
    }
  }, [clearError, setError, state.sessionId, updateState]);

  /**
   * 流式分析（LLM 模式）：
   * 调用 /api/analyze/stream，通过 SSE 实时接收 LLM Markdown 输出。
   *
   * @param onChunk        每收到一个 markdown 文本块时调用
   * @param onMeta         收到结构化元数据时调用
   * @param onStatus       收到状态消息时调用
   * @param onDone         流结束时调用
   */
  const runAnalysisStream = useCallback(async (
    mapping: FieldMappingRequest,
    config: AnalysisConfig,
    forceProceed: boolean = false,
    callbacks: {
      onChunk?: (text: string) => void;
      onMeta?: (meta: Record<string, unknown>) => void;
      onStatus?: (text: string) => void;
      onDone?: () => void;
    } = {},
  ): Promise<void> => {
    if (!state.sessionId) {
      throw new Error('当前没有可用的上传会话，请重新上传文件。');
    }

    setIsLoading(true);
    clearError();
    updateState({ step: 'analyzing' });

    try {
      const params = new URLSearchParams({
        session_id: state.sessionId,
        force_proceed: String(forceProceed),
      });
      const { param_config, ...analysisConfig } = config;
      const response = await fetch(`${API_BASE}/api/analyze/stream?${params}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mapping,
          analysis_config: analysisConfig,
          param_config,
        }),
      });

      if (!response.ok) {
        const errMsg = await readError(response, '流式分析失败');
        throw new Error(errMsg);
      }

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      if (!reader) throw new Error('流式响应不可读');

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const raw = line.slice(6).trim();
          if (!raw) continue;

          try {
            const event = JSON.parse(raw);
            switch (event.type) {
              case 'status':
                callbacks.onStatus?.(event.text ?? '');
                break;
              case 'meta':
                callbacks.onMeta?.(event);
                break;
              case 'markdown':
                callbacks.onChunk?.(event.text ?? '');
                break;
              case 'done':
                callbacks.onDone?.();
                break;
              case 'error':
                setError(event.text ?? '分析失败');
                callbacks.onDone?.();
                break;
            }
          } catch {
            // 非 JSON 行（分隔符等），忽略
          }
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : '流式分析失败';
      setError(message);
      callbacks.onDone?.();
    } finally {
      setIsLoading(false);
    }
  }, [clearError, setError, state.sessionId, updateState]);

  const destroySession = useCallback(async (): Promise<void> => {
    if (!state.sessionId) {
      reset();
      return;
    }

    try {
      await fetch(`${API_BASE}/api/session/${state.sessionId}`, { method: 'DELETE' });
    } catch (error) {
      console.warn('销毁 Session 失败：', error);
    }

    reset();
  }, [reset, state.sessionId]);

  return {
    state,
    isLoading,
    uploadFile,
    validateMapping,
    detectJsonKeys,
    runAnalysis,
    runAnalysisStream,
    destroySession,
    reset,
    setError,
    clearError,
    updateState,
  };
}
