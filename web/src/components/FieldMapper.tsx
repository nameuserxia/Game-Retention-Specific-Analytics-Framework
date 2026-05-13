import { useState, useCallback, useMemo } from 'react';
import type {
  AnalysisConfig,
  AnalysisContext,
  FieldMappingRequest,
  JsonKeyDiscoveryResponse,
  JsonKeyInfo,
  ParamMappingConfig,
  SchemaDiscoveryResponse,
} from '../types/api';

interface FieldMapperProps {
  schema: SchemaDiscoveryResponse;
  onValidate: (mapping: FieldMappingRequest, config: AnalysisConfig) => void;
  onDetectJsonKeys: (jsonParamsCol: string) => Promise<JsonKeyDiscoveryResponse>;
  isLoading: boolean;
}

const REQUIRED_FIELDS = [
  { key: 'user_id', label: '用户 ID', help: '唯一标识用户的列，例如 distinct_id、user_id、account_id。' },
  { key: 'event_time', label: '事件时间', help: '精确到秒的事件时间，用于行为路径排序。' },
  { key: 'event_date', label: '事件日期', help: '事件所在日期，可以直接选日期列。' },
  { key: 'reg_date', label: '注册日期', help: '用户注册或首次活跃日期，用于划分 cohort。' },
  { key: 'event_name', label: '事件名称', help: '事件类型字段，例如 login、level_start、ad_show。' },
] as const;

const OPTIONAL_FIELDS = [
  { key: 'country', label: '国家/地区', help: '用于按地区拆分留存。' },
  { key: 'channel', label: '渠道来源', help: '用于按买量渠道或自然量拆分留存。' },
  { key: 'json_params', label: 'JSON 参数列', help: '包含事件参数的 JSON 字符串列，例如 pri_params。' },
] as const;

type SelectableMappingKey = Exclude<keyof FieldMappingRequest, 'extra_fields'>;

function unique(values: string[]) {
  return Array.from(new Set(values.filter(Boolean)));
}

export function FieldMapper({ schema, onValidate, onDetectJsonKeys, isLoading }: FieldMapperProps) {
  const suggestedRegStart = schema.stats?.suggested_reg_start || '';
  const suggestedRegEnd = schema.stats?.suggested_reg_end || '';

  const [mapping, setMapping] = useState<Partial<FieldMappingRequest>>({
    user_id: schema.suggestions?.user_id?.[0] || '',
    event_time: schema.suggestions?.event_time?.[0] || '',
    event_date: schema.suggestions?.event_date?.[0] || '',
    reg_date: schema.suggestions?.reg_date?.[0] || '',
    event_name: schema.suggestions?.event_name?.[0] || '',
    country: schema.suggestions?.country?.[0] || '',
    channel: schema.suggestions?.channel?.[0] || '',
    json_params: schema.columns.includes('pri_params') ? 'pri_params' : '',
  });

  const [enabledOptional, setEnabledOptional] = useState({
    country: Boolean(schema.suggestions?.country?.[0]),
    channel: Boolean(schema.suggestions?.channel?.[0]),
    json_params: schema.columns.includes('pri_params'),
  });

  const [jsonKeys, setJsonKeys] = useState<JsonKeyInfo[]>([]);
  const [jsonDetecting, setJsonDetecting] = useState(false);
  const [jsonError, setJsonError] = useState('');
  const [showAllColumns, setShowAllColumns] = useState(false);
  const [columnSearch, setColumnSearch] = useState('');
  const [paramConfig, setParamConfig] = useState<ParamMappingConfig>({
    json_params_col: schema.columns.includes('pri_params') ? 'json_params' : undefined,
    progress_key: '',
    result_key: '',
    numeric_keys: [],
    segment_keys: [],
    relevant_events: [],
  });
  const [relevantEventsInput, setRelevantEventsInput] = useState('');

  const [config, setConfig] = useState<AnalysisConfig>({
    reg_start: suggestedRegStart,
    reg_end: suggestedRegEnd,
    retention_days: 1,
    min_sample_size: 30,
    cohort_freq: 'W',
    max_days: 30,
    segment_by_country: Boolean(schema.suggestions?.country?.[0]),
    segment_by_channel: Boolean(schema.suggestions?.channel?.[0]),
    game_genre: 'casual',
  });
  const [analysisContext, setAnalysisContext] = useState<AnalysisContext>({
    game_name: '',
    gameplay: '',
    game_genre: '其他',
    recent_events: [] as string[],
    main_concern: '',
  });
  const [recentEventsInput, setRecentEventsInput] = useState('');

  const requiredReady = useMemo(() => REQUIRED_FIELDS.every(item => Boolean(mapping[item.key])), [mapping]);
  const missingRequired = REQUIRED_FIELDS.filter(item => !mapping[item.key]).map(item => item.label);
  const selectedColumns = useMemo(() => Object.values(mapping).filter(Boolean).length, [mapping]);

  const keyOptions = useMemo(() => jsonKeys.map(item => item.key), [jsonKeys]);
  const filteredColumnInfos = useMemo(() => {
    const keyword = columnSearch.trim().toLowerCase();
    const matched = keyword
      ? schema.column_infos.filter(column => column.name.toLowerCase().includes(keyword))
      : schema.column_infos;
    return showAllColumns ? matched : matched.slice(0, 8);
  }, [columnSearch, schema.column_infos, showAllColumns]);
  const eventNameSamples = useMemo(() => {
    const eventCol = mapping.event_name;
    if (!eventCol) return [];
    return unique(
      schema.preview
        .map(row => String(row[eventCol] ?? '').trim())
        .filter(Boolean)
    ).slice(0, 8);
  }, [mapping.event_name, schema.preview]);

  const handleMappingChange = useCallback((field: SelectableMappingKey, value: string) => {
    setMapping(prev => ({ ...prev, [field]: value }));
    if (field === 'json_params') {
      setJsonKeys([]);
      setJsonError('');
      setParamConfig(prev => ({ ...prev, json_params_col: value ? 'json_params' : undefined }));
    }
  }, []);

  const handleConfigChange = useCallback(<K extends keyof AnalysisConfig>(field: K, value: AnalysisConfig[K]) => {
    setConfig(prev => ({ ...prev, [field]: value }));
  }, []);

  const handleOptionalToggle = useCallback((field: 'country' | 'channel' | 'json_params', checked: boolean) => {
    setEnabledOptional(prev => ({ ...prev, [field]: checked }));
    if (!checked) {
      setMapping(prev => ({ ...prev, [field]: '' }));
      if (field === 'json_params') {
        setJsonKeys([]);
        setRelevantEventsInput('');
        setParamConfig({ json_params_col: undefined, progress_key: '', result_key: '', numeric_keys: [], segment_keys: [], relevant_events: [] });
      }
    }
    if (field === 'country') handleConfigChange('segment_by_country', checked);
    if (field === 'channel') handleConfigChange('segment_by_channel', checked);
  }, [handleConfigChange]);

  const detectJsonKeys = useCallback(async () => {
    if (!mapping.json_params) return;
    setJsonDetecting(true);
    setJsonError('');
    try {
      const result = await onDetectJsonKeys(mapping.json_params);
      setJsonKeys(result.keys);
      const progress = result.keys.find(item => item.suggested_role === 'progress')?.key || '';
      const state = result.keys.find(item => item.suggested_role === 'result')?.key || '';
      const numeric = result.keys.filter(item => item.suggested_role === 'numeric').slice(0, 2).map(item => item.key);
      setParamConfig(prev => ({
        json_params_col: 'json_params',
        progress_key: progress,
        result_key: state,
        numeric_keys: numeric,
        segment_keys: progress ? [progress] : [],
        relevant_events: prev.relevant_events || [],
      }));
    } catch (error) {
      setJsonError(error instanceof Error ? error.message : 'JSON Key 探测失败');
    } finally {
      setJsonDetecting(false);
    }
  }, [mapping.json_params, onDetectJsonKeys]);

  const updateParamRole = useCallback((role: 'progress_key' | 'result_key', value: string) => {
    setParamConfig(prev => ({ ...prev, [role]: value }));
  }, []);

  const toggleParamList = useCallback((field: 'numeric_keys' | 'segment_keys', key: string, checked: boolean) => {
    setParamConfig(prev => ({
      ...prev,
      [field]: checked ? unique([...(prev[field] || []), key]) : (prev[field] || []).filter(item => item !== key),
    }));
  }, []);

  const updateRelevantEvents = useCallback((value: string) => {
    setRelevantEventsInput(value);
    const events = unique(value.split(/[,，\n]/).map(item => item.trim()));
    setParamConfig(prev => ({ ...prev, relevant_events: events }));
  }, []);

  const updateAnalysisContext = useCallback((field: 'game_name' | 'gameplay' | 'game_genre' | 'main_concern', value: string) => {
    setAnalysisContext(prev => ({ ...prev, [field]: value }));
  }, []);

  const updateRecentEvents = useCallback((value: string) => {
    setRecentEventsInput(value);
    const events = unique(value.split(/[,，\n]/).map(item => item.trim()));
    setAnalysisContext(prev => ({ ...prev, recent_events: events }));
  }, []);

  const addRelevantEvent = useCallback((eventName: string) => {
    const events = unique([...(paramConfig.relevant_events || []), eventName]);
    setRelevantEventsInput(events.join(', '));
    setParamConfig(prev => ({ ...prev, relevant_events: events }));
  }, [paramConfig.relevant_events]);

  const handleSubmit = useCallback(() => {
    if (!requiredReady) return;

    const fullMapping: FieldMappingRequest = {
      user_id: mapping.user_id || '',
      event_time: mapping.event_time || '',
      event_date: mapping.event_date || '',
      reg_date: mapping.reg_date || '',
      event_name: mapping.event_name || '',
      country: enabledOptional.country ? mapping.country || undefined : undefined,
      channel: enabledOptional.channel ? mapping.channel || undefined : undefined,
      json_params: enabledOptional.json_params ? mapping.json_params || undefined : undefined,
    };

    onValidate(fullMapping, {
      ...config,
      segment_by_country: enabledOptional.country && Boolean(mapping.country),
      segment_by_channel: enabledOptional.channel && Boolean(mapping.channel),
      param_config: enabledOptional.json_params && mapping.json_params ? paramConfig : undefined,
      analysis_context: {
        ...analysisContext,
        game_name: analysisContext.game_name || undefined,
        gameplay: analysisContext.gameplay || undefined,
        game_genre: analysisContext.game_genre || undefined,
        main_concern: analysisContext.main_concern || undefined,
      },
    });
  }, [analysisContext, config, enabledOptional, mapping, onValidate, paramConfig, requiredReady]);

  const renderSelect = (field: SelectableMappingKey, required: boolean, disabled = false) => {
    const suggested = schema.suggestions?.[field]?.[0];
    const selected = mapping[field] || '';

    return (
      <select
        value={selected}
        onChange={(event) => handleMappingChange(field, event.target.value)}
        className={!selected && required ? 'empty' : ''}
        disabled={disabled}
      >
        <option value="">请选择数据列</option>
        {schema.columns.map(column => (
          <option key={column} value={column}>
            {column}{column === suggested ? '（推荐）' : ''}
          </option>
        ))}
      </select>
    );
  };

  return (
    <div className="field-mapper">
      <section className="panel file-panel">
        <div>
          <p className="panel-kicker">已解析文件</p>
          <h2>{schema.file_name}</h2>
          <p>{schema.total_rows.toLocaleString()} 行 x {schema.total_columns} 列，文件大小 {schema.file_size_mb} MB</p>
        </div>
        <div className="selected-count">
          <strong>{selectedColumns}</strong>
          <span>个字段已选择</span>
        </div>
      </section>

      <section className="panel">
        <div className="section-title">
          <div>
            <p className="panel-kicker">第一步：主列映射</p>
            <h3>选择留存分析必需字段</h3>
          </div>
          {!requiredReady && <span className="missing-tip">还缺：{missingRequired.join('、')}</span>}
        </div>
        <div className="field-list">
          {REQUIRED_FIELDS.map(item => (
            <div className="field-row" key={item.key}>
              <div>
                <label>{item.label}<span className="required">*</span></label>
                <p>{item.help}</p>
              </div>
              {renderSelect(item.key, true)}
            </div>
          ))}
        </div>
      </section>

      <section className="panel">
        <div className="section-title">
          <div>
            <p className="panel-kicker">第二步：可选维度与 JSON 参数</p>
            <h3>勾选后可加入分群和诊断流程</h3>
          </div>
        </div>
        <div className="optional-grid">
          {OPTIONAL_FIELDS.map(item => {
            const checked = enabledOptional[item.key];
            return (
              <div className={`optional-card ${checked ? 'enabled' : ''}`} key={item.key}>
                <label className="optional-toggle">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(event) => handleOptionalToggle(item.key, event.target.checked)}
                  />
                  <span>{item.label}</span>
                </label>
                <p>{item.help}</p>
                {renderSelect(item.key, false, !checked)}
              </div>
            );
          })}
        </div>
      </section>

      {enabledOptional.json_params && mapping.json_params && (
        <section className="panel">
          <div className="section-title">
            <div>
              <p className="panel-kicker">第三步：JSON Key 映射</p>
              <h3>声明参数 Key 的业务用途</h3>
            </div>
            <button type="button" className="secondary-btn" onClick={detectJsonKeys} disabled={jsonDetecting}>
              {jsonDetecting ? '探测中...' : '自动探测 Key'}
            </button>
          </div>

          {jsonError && <div className="json-error">{jsonError}</div>}

          {jsonKeys.length > 0 ? (
            <>
              <div className="relevant-events-box">
                <label className="config-row">
                  <span>相关事件 relevant_events</span>
                  <textarea
                    value={relevantEventsInput}
                    onChange={(event) => updateRelevantEvents(event.target.value)}
                    placeholder="只填这些 JSON Key 应该出现的 event_name，多个用逗号分隔，例如 ugd_level_start, ugd_level_pass, ugd_level_fail"
                    rows={3}
                  />
                </label>
                {eventNameSamples.length > 0 && (
                  <div className="event-samples">
                    <span>样例事件</span>
                    {eventNameSamples.map(eventName => (
                      <button type="button" key={eventName} onClick={() => addRelevantEvent(eventName)}>
                        {eventName}
                      </button>
                    ))}
                  </div>
                )}
                <p>没有配置相关事件时，系统只展示虚拟字段全量覆盖率，不会提示“参数缺失”。</p>
              </div>

              <div className="param-role-grid">
                <label className="config-row">
                  <span>进度维度</span>
                  <select value={paramConfig.progress_key || ''} onChange={(event) => updateParamRole('progress_key', event.target.value)}>
                    <option value="">不指定</option>
                    {keyOptions.map(key => <option key={key} value={key}>{key}</option>)}
                  </select>
                </label>
                <label className="config-row">
                  <span>结果状态</span>
                  <select value={paramConfig.result_key || ''} onChange={(event) => updateParamRole('result_key', event.target.value)}>
                    <option value="">不指定</option>
                    {keyOptions.map(key => <option key={key} value={key}>{key}</option>)}
                  </select>
                </label>
              </div>

              <div className="key-table">
                {jsonKeys.slice(0, 24).map(item => (
                  <div className="key-row" key={item.key}>
                    <div>
                      <strong>{item.key}</strong>
                      <span>填充率 {(item.fill_rate * 100).toFixed(1)}% · 样例 {item.sample_values.slice(0, 2).map(String).join(' / ') || '-'}</span>
                    </div>
                    <label>
                      <input
                        type="checkbox"
                        checked={paramConfig.numeric_keys.includes(item.key)}
                        onChange={(event) => toggleParamList('numeric_keys', item.key, event.target.checked)}
                      />
                      数值指标
                    </label>
                    <label>
                      <input
                        type="checkbox"
                        checked={paramConfig.segment_keys.includes(item.key)}
                        onChange={(event) => toggleParamList('segment_keys', item.key, event.target.checked)}
                      />
                      分群维度
                    </label>
                  </div>
                ))}
              </div>
            </>
          ) : (
            <p className="empty-json">点击“自动探测 Key”后，可以把 level_id 声明为进度维度，把 state 声明为结果状态，把 step 声明为数值指标。</p>
          )}
        </section>
      )}

      <section className="panel">
        <div className="section-title">
          <div>
            <p className="panel-kicker">业务上下文</p>
            <h3>补充给 AI 报告使用的信息</h3>
          </div>
        </div>
        <div className="config-grid">
          <label className="config-row">
            <span>游戏名称</span>
            <input value={analysisContext.game_name || ''} onChange={(event) => updateAnalysisContext('game_name', event.target.value)} />
          </label>
          <label className="config-row">
            <span>游戏类型</span>
            <select value={analysisContext.game_genre || '其他'} onChange={(event) => updateAnalysisContext('game_genre', event.target.value)}>
              <option value="SLG">SLG</option>
              <option value="Roguelike">Roguelike</option>
              <option value="Match3">Match3</option>
              <option value="MMO">MMO</option>
              <option value="Idle">Idle</option>
              <option value="FPS">FPS</option>
              <option value="棋牌">棋牌</option>
              <option value="其他">其他</option>
            </select>
          </label>
          <label className="config-row">
            <span>当前最担心的问题</span>
            <input value={analysisContext.main_concern || ''} onChange={(event) => updateAnalysisContext('main_concern', event.target.value)} />
          </label>
        </div>
        <label className="config-row context-textarea">
          <span>游戏玩法</span>
          <textarea rows={2} value={analysisContext.gameplay || ''} onChange={(event) => updateAnalysisContext('gameplay', event.target.value)} />
        </label>
        <label className="config-row context-textarea">
          <span>最近运营事件</span>
          <textarea
            rows={2}
            value={recentEventsInput}
            onChange={(event) => updateRecentEvents(event.target.value)}
            placeholder="例如：新版本上线，广告投放变化，活动改版，BUG，崩溃问题，买量变化"
          />
        </label>
        <p>这些内容不会参与留存计算，只会进入 AI 报告上下文。</p>
      </section>

      <section className="panel">
        <div className="section-title">
          <div>
            <p className="panel-kicker">分析配置</p>
            <h3>设置注册窗口和留存口径</h3>
          </div>
        </div>
        <div className="config-grid">
          <label className="config-row">
            <span>注册窗口开始</span>
            <input type="date" value={config.reg_start} onChange={(event) => handleConfigChange('reg_start', event.target.value)} />
          </label>
          <label className="config-row">
            <span>注册窗口结束</span>
            <input type="date" value={config.reg_end} onChange={(event) => handleConfigChange('reg_end', event.target.value)} />
          </label>
          <label className="config-row">
            <span>留存天数</span>
            <input type="number" min={1} max={365} value={config.retention_days} onChange={(event) => handleConfigChange('retention_days', Number(event.target.value))} />
          </label>
          <label className="config-row">
            <span>Cohort 频率</span>
            <select value={config.cohort_freq} onChange={(event) => handleConfigChange('cohort_freq', event.target.value as AnalysisConfig['cohort_freq'])}>
              <option value="D">按天</option>
              <option value="W">按周</option>
              <option value="M">按月</option>
            </select>
          </label>
          <label className="config-row">
            <span>观察天数上限</span>
            <input type="number" min={7} max={365} value={config.max_days} onChange={(event) => handleConfigChange('max_days', Number(event.target.value))} />
          </label>
          <label className="config-row">
            <span>最小样本量</span>
            <input type="number" min={1} value={config.min_sample_size} onChange={(event) => handleConfigChange('min_sample_size', Number(event.target.value))} />
          </label>
          <label className="config-row">
            <span>游戏类型 Benchmark</span>
            <select value={config.game_genre || 'casual'} onChange={(event) => handleConfigChange('game_genre', event.target.value)}>
              <option value="casual">休闲游戏</option>
              <option value="competitive">竞技游戏</option>
              <option value="mmo">MMO</option>
            </select>
          </label>
        </div>
      </section>

      <section className="panel preview-panel">
        <div className="section-title">
          <div>
            <p className="panel-kicker">列名预览</p>
            <h3>字段样例</h3>
          </div>
          <div className="column-tools">
            <input
              type="search"
              value={columnSearch}
              onChange={(event) => setColumnSearch(event.target.value)}
              placeholder="搜索列名"
            />
            <button type="button" onClick={() => setShowAllColumns(prev => !prev)}>
              {showAllColumns ? '收起' : `展开全部 ${schema.column_infos.length} 列`}
            </button>
          </div>
        </div>
        <div className="column-preview">
          {filteredColumnInfos.map(column => (
            <div className="column-chip" key={column.name}>
              <strong>{column.name}</strong>
              <span>{column.dtype}</span>
              <small>{column.sample_values.slice(0, 2).map(String).join(' / ') || '无样例'}</small>
            </div>
          ))}
        </div>
        {filteredColumnInfos.length === 0 && <p className="empty-json">没有匹配的列名</p>}
      </section>

      <div className="actions">
        <button className="validate-btn" onClick={handleSubmit} disabled={!requiredReady || isLoading}>
          {isLoading ? '正在预校验...' : '预校验字段并继续'}
        </button>
      </div>

      <style>{`
        .field-mapper { display: grid; gap: 18px; }
        .panel { background: #fff; border: 1px solid #dfe6ef; border-radius: 8px; padding: 22px; box-shadow: 0 12px 28px rgba(26, 38, 62, 0.06); }
        .file-panel, .section-title { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }
        .panel-kicker { margin: 0 0 6px; color: #0f766e; font-size: 13px; font-weight: 800; }
        .panel h2, .panel h3 { margin: 0; color: #172033; }
        .panel h2 { font-size: 22px; }
        .panel h3 { font-size: 18px; }
        .panel p { margin: 8px 0 0; color: #5d6b82; line-height: 1.7; }
        .selected-count { min-width: 130px; padding: 12px; text-align: center; color: #0f5132; background: #e8f7ef; border: 1px solid #b7e4cc; border-radius: 8px; }
        .selected-count strong { display: block; font-size: 26px; }
        .selected-count span { font-size: 13px; }
        .missing-tip, .json-error { padding: 6px 10px; color: #9f1239; background: #fff1f2; border: 1px solid #fecdd3; border-radius: 6px; font-size: 13px; }
        .field-list { display: grid; gap: 12px; }
        .field-row { display: grid; grid-template-columns: minmax(220px, 0.8fr) minmax(260px, 1.2fr); gap: 16px; align-items: center; padding: 14px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; }
        label { font-weight: 800; }
        .required { color: #dc2626; margin-left: 3px; }
        .field-row p, .optional-card p { margin: 4px 0 0; font-size: 13px; color: #667085; }
        select, input, textarea { min-height: 40px; width: 100%; padding: 8px 11px; color: #172033; background: #fff; border: 1px solid #c9d4e3; border-radius: 6px; outline: none; }
        textarea { resize: vertical; line-height: 1.6; font-family: inherit; }
        select:focus, input:focus, textarea:focus { border-color: #0f766e; box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.12); }
        select.empty { border-color: #f43f5e; background: #fff7f8; }
        select:disabled { color: #98a2b3; background: #eef2f6; }
        .optional-grid, .param-role-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
        .optional-card { padding: 16px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; }
        .optional-card.enabled { background: #f0fbf8; border-color: #8dd8c6; }
        .optional-toggle { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; cursor: pointer; }
        .optional-toggle input, .key-row input { width: 18px; height: 18px; accent-color: #0f766e; }
        .config-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
        .config-row { display: grid; gap: 7px; color: #344054; font-size: 14px; }
        .context-textarea { margin-top: 14px; }
        .secondary-btn { min-height: 38px; padding: 0 16px; color: #0f766e; background: #e8f7ef; border: 1px solid #b7e4cc; border-radius: 6px; cursor: pointer; font-weight: 800; }
        .empty-json { padding: 14px; background: #f8fafc; border: 1px dashed #c9d4e3; border-radius: 8px; }
        .relevant-events-box { margin-bottom: 16px; padding: 14px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; }
        .relevant-events-box p { margin-top: 10px; font-size: 13px; }
        .event-samples { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-top: 10px; }
        .event-samples span { color: #667085; font-size: 13px; font-weight: 800; }
        .event-samples button { max-width: 180px; min-height: 30px; padding: 0 10px; overflow: hidden; color: #0f5132; text-overflow: ellipsis; white-space: nowrap; background: #e8f7ef; border: 1px solid #b7e4cc; border-radius: 6px; cursor: pointer; }
        .key-table { display: grid; gap: 10px; margin-top: 16px; }
        .key-row { display: grid; grid-template-columns: 1fr 110px 110px; gap: 12px; align-items: center; padding: 12px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; }
        .key-row strong, .key-row span { display: block; }
        .key-row span { margin-top: 4px; color: #667085; font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .key-row label { display: flex; gap: 6px; align-items: center; font-size: 13px; }
        .column-preview { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
        .column-tools { display: flex; gap: 8px; align-items: center; }
        .column-tools input { width: 180px; }
        .column-tools button { min-height: 40px; padding: 0 12px; color: #0f766e; background: #e8f7ef; border: 1px solid #b7e4cc; border-radius: 6px; cursor: pointer; font-weight: 800; }
        .column-chip { min-height: 90px; padding: 12px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; }
        .column-chip strong, .column-chip span, .column-chip small { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .column-chip span { margin-top: 5px; color: #0f766e; font-size: 12px; font-weight: 800; }
        .column-chip small { margin-top: 8px; color: #667085; }
        .actions { display: flex; justify-content: flex-end; padding-bottom: 6px; }
        .validate-btn { min-height: 46px; padding: 0 28px; color: #fff; background: #0f766e; border: 0; border-radius: 6px; cursor: pointer; font-size: 16px; font-weight: 800; }
        .validate-btn:disabled, .secondary-btn:disabled { opacity: 0.55; cursor: not-allowed; }
        @media (max-width: 860px) {
          .file-panel, .section-title { flex-direction: column; }
          .field-row, .optional-grid, .config-grid, .column-preview, .param-role-grid, .key-row { grid-template-columns: 1fr; }
        }
      `}</style>
    </div>
  );
}
