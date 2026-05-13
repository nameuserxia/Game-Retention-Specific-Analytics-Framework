/**
 * web/src/types/api.ts
 * API 类型定义（与后端 Pydantic 模型对应）
 */

// ============================================================
// 字段映射模型
// ============================================================

export interface FieldMappingRequest {
  user_id: string;
  event_time: string;
  event_date: string;
  reg_date: string;
  event_name: string;
  country?: string;
  channel?: string;
  json_params?: string;
  extra_fields?: Record<string, string>;
}

export interface ParamMappingConfig {
  json_params_col?: string;
  progress_key?: string;
  result_key?: string;
  numeric_keys: string[];
  segment_keys: string[];
  relevant_events: string[];
}

export interface AnalysisContext {
  game_name?: string;
  gameplay?: string;
  game_genre?: string;
  recent_events: string[];
  main_concern?: string;
  extra?: Record<string, unknown>;
}

export interface JsonKeyInfo {
  key: string;
  count: number;
  fill_rate: number;
  sample_values: unknown[];
  suggested_role: 'progress' | 'result' | 'numeric' | 'segment' | string;
}

export interface JsonKeyDiscoveryResponse {
  session_id: string;
  json_params_col: string;
  total_sampled_rows: number;
  parsed_rows: number;
  parse_error_rows: number;
  keys: JsonKeyInfo[];
}

// ============================================================
// 分析配置模型
// ============================================================

export interface AnalysisConfig {
  reg_start: string;        // YYYY-MM-DD
  reg_end: string;          // YYYY-MM-DD
  retention_days: number;    // D+N
  min_sample_size: number;
  cohort_freq: 'D' | 'W' | 'M';
  max_days: number;
  segment_by_country: boolean;
  segment_by_channel: boolean;
  game_genre?: 'casual' | 'competitive' | 'mmo' | string;
  param_config?: ParamMappingConfig;
  analysis_context?: AnalysisContext;
  ai_enabled?: boolean;
  dynamic_dimensions?: string[][];
  funnel_steps?: string[];
  dynamic_retention_days?: number[];
}

// ============================================================
// Schema Discovery 响应
// ============================================================

export interface ColumnInfo {
  name: string;
  dtype: string;
  nullable: boolean;
  sample_values: unknown[];
}

export interface SchemaDiscoveryResponse {
  session_id: string;
  file_name: string;
  expires_at?: string;
  total_rows: number;
  total_columns: number;
  columns: string[];
  column_infos: ColumnInfo[];
  preview: Record<string, string>[];
  suggestions: Record<string, string[]>;
  stats?: {
    suggested_reg_start?: string;
    suggested_reg_end?: string;
    date_ranges?: Record<string, { min: string; max: string }>;
    [key: string]: unknown;
  };
  file_size_mb: number;
}

// ============================================================
// 预校验响应
// ============================================================

export interface DateParseResult {
  success: boolean;
  null_rate: number;
  failed_count: number;
  failed_examples: string[];
  inferred_format?: string;
}

export interface ValidationResponse {
  session_id: string;
  success: boolean;
  can_proceed: boolean;
  parse_results: Record<string, DateParseResult>;
  errors: string[];
  warnings: string[];
  stats: Record<string, unknown>;
}

// ============================================================
// 分析结果响应
// ============================================================

export interface RetentionResult {
  segment: string;
  n_total: number;
  n_retained: number;
  retention_rate: number;
  note: string;
}

export interface AnalysisSummary {
  reg_start: string;
  reg_end: string;
  retention_days: number;
  n_total: number;
  n_retained: number;
  n_churn: number;
  retention_rate: number;
}

export interface TopPath {
  rank: number;
  path: string;
  count: number;
  pct: number;
}

export interface AnalysisResponse {
  session_id: string;
  success: boolean;
  message: string;
  summary: AnalysisSummary;
  retention_result: RetentionResult[];
  cohort_headers: string[];
  cohort_matrix: unknown[][];
  churn_users_count: number;
  retained_users_count: number;
  country_retention: RetentionResult[];
  channel_retention: RetentionResult[];
  top_paths: TopPath[];
  report_markdown: string;
  sanity_check_report: Record<string, unknown>;
  diagnostics: Record<string, unknown>;
  virtual_fields: string[];
  report_id?: string;
  report_title?: string;
  report_path?: string;
  llm_used: boolean;
  llm_fallback_reason?: string;
  dynamic_retention: Array<{
    dimensions: string[];
    groups: Array<{
      group_key: string;
      cohort_size: number;
      retention: Record<string, number>;
      gap_vs_overall: Record<string, number>;
      sample_warning: boolean;
    }>;
    warnings: string[];
  }>;
  funnel_analysis?: {
    steps: Array<{
      event: string;
      users: number;
      step_conversion_rate: number;
      overall_conversion_rate: number;
      dropoff_users: number;
      dropoff_rate: number;
    }>;
    warnings: string[];
  } | null;
}

// ============================================================
// Session 管理
// ============================================================

export interface SessionInfo {
  session_id: string;
  file_name?: string;
  file_size_mb?: number;
  total_rows?: number;
  created_at: string;
  expires_at: string;
  status: 'pending' | 'ready' | 'analyzing' | 'done' | 'expired' | 'error';
}

export interface CleanupResponse {
  success: boolean;
  session_id: string;
  files_deleted: string[];
}

// ============================================================
// 分析流程状态
// ============================================================

export type AnalysisStep = 'idle' | 'upload' | 'mapping' | 'validation' | 'analyzing' | 'done';

export interface AnalysisState {
  step: AnalysisStep;
  sessionId?: string;
  schema?: SchemaDiscoveryResponse;
  mapping?: FieldMappingRequest;
  analysisConfig?: AnalysisConfig;
  validation?: ValidationResponse;
  result?: AnalysisResponse;
  error?: string;
}

// ============================================================
// API 错误响应
// ============================================================

export interface APIError {
  detail: string;
}

// ============================================================
// 默认值
// ============================================================

export const DEFAULT_ANALYSIS_CONFIG: AnalysisConfig = {
  reg_start: '',
  reg_end: '',
  retention_days: 1,
  min_sample_size: 30,
  cohort_freq: 'W',
  max_days: 30,
  segment_by_country: false,
  segment_by_channel: false,
  game_genre: 'casual',
};

export const REQUIRED_FIELDS = [
  'user_id',
  'event_time',
  'event_date',
  'reg_date',
  'event_name',
] as const;

export const OPTIONAL_FIELDS = [
  'country',
  'channel',
] as const;

export const ALL_FIELDS = [...REQUIRED_FIELDS, ...OPTIONAL_FIELDS] as const;
