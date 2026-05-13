import { useState } from 'react';
import type { ValidationResponse, AnalysisConfig, FieldMappingRequest } from '../types/api';

interface ValidationResultProps {
  validation: ValidationResponse;
  mapping: FieldMappingRequest;
  config: AnalysisConfig;
  onContinue: (forceProceed: boolean, aiMode: boolean) => void;
  onBack: () => void;
  isLoading: boolean;
}

const FIELD_LABELS: Record<string, string> = {
  user_id: '用户 ID',
  event_time: '事件时间',
  event_date: '事件日期',
  reg_date: '注册日期',
  event_name: '事件名称',
  country: '国家/地区',
  channel: '渠道来源',
};

export function ValidationResult({
  validation,
  mapping,
  config,
  onContinue,
  onBack,
  isLoading,
}: ValidationResultProps) {
  const [aiMode, setAiMode] = useState(false);
  const { parse_results, errors, warnings, can_proceed } = validation;

  return (
    <div className="validation-result">
      <section className={`status-header ${can_proceed ? 'success' : 'error'}`}>
        <div className="status-icon">{can_proceed ? '✓' : '!'}</div>
        <div className="status-text">
          <h2>{can_proceed ? '预校验通过' : '预校验发现问题'}</h2>
          <p>
            {can_proceed
              ? '字段映射和日期解析可用于后续分析。'
              : '请先修正下方问题；如果你确认数据可用，也可以强制继续。'}
          </p>
        </div>
      </section>

      <section className="panel">
        <div className="section-title">
          <p>日期解析结果</p>
          <span>{Object.keys(parse_results).length} 个日期字段</span>
        </div>
        <div className="table-wrapper">
          <table className="parse-table">
            <thead>
              <tr>
                <th>标准字段</th>
                <th>原始列名</th>
                <th>状态</th>
                <th>空值/失败率</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(parse_results).map(([field, result]) => {
                const originalName = mapping[field as keyof FieldMappingRequest];
                const displayName = typeof originalName === 'string' ? originalName : '';
                return (
                  <tr key={field}>
                    <td>{FIELD_LABELS[field] || field}</td>
                    <td><code>{displayName}</code></td>
                    <td>
                      <span className={`badge ${result.success ? 'success' : 'error'}`}>
                        {result.success ? '成功' : '失败'}
                      </span>
                    </td>
                    <td>
                      {(result.null_rate * 100).toFixed(1)}%
                      {result.failed_count > 0 && <span className="muted">（{result.failed_count} 行）</span>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {Object.entries(parse_results).map(([field, result]) => {
          if (!result.success || !result.failed_examples?.length) return null;
          return (
            <div key={field} className="failed-examples">
              <strong>{FIELD_LABELS[field] || field} 解析失败样例</strong>
              <ul>
                {result.failed_examples.slice(0, 3).map((value, index) => (
                  <li key={index}><code>{value}</code></li>
                ))}
              </ul>
            </div>
          );
        })}
      </section>

      {errors.length > 0 && (
        <section className="panel message error">
          <h3>错误</h3>
          <ul>{errors.map((error, index) => <li key={index}>{error}</li>)}</ul>
        </section>
      )}

      {warnings.length > 0 && (
        <section className="panel message warning">
          <h3>警告</h3>
          <ul>{warnings.map((warning, index) => <li key={index}>{warning}</li>)}</ul>
        </section>
      )}

      <section className="panel">
        <div className="section-title">
          <p>分析配置预览</p>
        </div>
        <div className="config-preview">
          <div>
            <span>注册窗口</span>
            <strong>{config.reg_start} 至 {config.reg_end}</strong>
          </div>
          <div>
            <span>留存定义</span>
            <strong>D+{config.retention_days}</strong>
          </div>
          <div>
            <span>Cohort</span>
            <strong>{config.cohort_freq === 'D' ? '按天' : config.cohort_freq === 'W' ? '按周' : '按月'}</strong>
          </div>
          <div>
            <span>观察上限</span>
            <strong>D+{config.max_days}</strong>
          </div>
          <div>
            <span>国家分群</span>
            <strong>{config.segment_by_country ? '开启' : '关闭'}</strong>
          </div>
          <div>
            <span>渠道分群</span>
            <strong>{config.segment_by_channel ? '开启' : '关闭'}</strong>
          </div>
        </div>
      </section>

      <div className="actions">
        <button className="btn-back" onClick={onBack} disabled={isLoading}>
          返回修改
        </button>
        <div className="continue-actions">
          {!can_proceed && (
            <button className="btn-force" onClick={() => onContinue(true, aiMode)} disabled={isLoading}>
              强制继续
            </button>
          )}
          <div className="ai-mode-row">
            <label className="ai-toggle">
              <input
                type="checkbox"
                checked={aiMode}
                onChange={e => setAiMode(e.target.checked)}
                disabled={isLoading}
              />
              <span>AI 分析模式（结构化报告，可自动降级）</span>
            </label>
            <button
              className="btn-continue"
              onClick={() => onContinue(false, aiMode)}
              disabled={!can_proceed || isLoading}
            >
              {isLoading ? '分析中...' : aiMode ? '开始 AI 分析' : '开始分析'}
            </button>
          </div>
        </div>
      </div>

      <style>{`
        .validation-result {
          display: grid;
          gap: 18px;
        }

        .status-header,
        .panel {
          background: #fff;
          border: 1px solid #dfe6ef;
          border-radius: 8px;
          box-shadow: 0 12px 28px rgba(26, 38, 62, 0.06);
        }

        .status-header {
          display: flex;
          align-items: center;
          gap: 16px;
          padding: 22px;
        }

        .status-header.success {
          border-color: #9ee2cd;
          background: #f0fbf8;
        }

        .status-header.error {
          border-color: #fecdd3;
          background: #fff1f2;
        }

        .status-icon {
          width: 44px;
          height: 44px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: #fff;
          background: #0f766e;
          border-radius: 50%;
          font-size: 24px;
          font-weight: 900;
        }

        .status-header.error .status-icon {
          background: #e11d48;
        }

        .status-text h2 {
          margin: 0 0 6px;
          font-size: 22px;
        }

        .status-text p {
          margin: 0;
          color: #5d6b82;
        }

        .panel {
          padding: 20px;
        }

        .section-title {
          display: flex;
          justify-content: space-between;
          gap: 12px;
          align-items: center;
          margin-bottom: 14px;
        }

        .section-title p {
          margin: 0;
          color: #172033;
          font-size: 18px;
          font-weight: 900;
        }

        .section-title span {
          color: #667085;
          font-size: 13px;
        }

        .table-wrapper {
          overflow-x: auto;
        }

        .parse-table {
          width: 100%;
          border-collapse: collapse;
        }

        .parse-table th,
        .parse-table td {
          padding: 11px 12px;
          text-align: left;
          border-bottom: 1px solid #edf1f6;
        }

        .parse-table th {
          color: #667085;
          background: #f8fafc;
          font-size: 13px;
        }

        code {
          padding: 2px 6px;
          background: #f3f6fa;
          border-radius: 4px;
          font-family: Consolas, "SFMono-Regular", monospace;
        }

        .badge {
          display: inline-flex;
          min-width: 48px;
          justify-content: center;
          padding: 3px 8px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 800;
        }

        .badge.success {
          color: #0f5132;
          background: #dff8f0;
        }

        .badge.error {
          color: #9f1239;
          background: #ffe4e6;
        }

        .muted {
          margin-left: 4px;
          color: #667085;
          font-size: 12px;
        }

        .failed-examples {
          margin-top: 14px;
          padding: 12px;
          background: #fff7f8;
          border: 1px solid #fecdd3;
          border-radius: 6px;
        }

        .failed-examples ul,
        .message ul {
          margin: 8px 0 0;
          padding-left: 20px;
        }

        .message h3 {
          margin: 0;
          font-size: 17px;
        }

        .message.error {
          color: #9f1239;
          background: #fff1f2;
          border-color: #fecdd3;
        }

        .message.warning {
          color: #854d0e;
          background: #fffbeb;
          border-color: #fde68a;
        }

        .config-preview {
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 12px;
        }

        .config-preview div {
          padding: 12px;
          background: #f8fafc;
          border: 1px solid #e2e8f0;
          border-radius: 8px;
        }

        .config-preview span {
          display: block;
          margin-bottom: 5px;
          color: #667085;
          font-size: 12px;
        }

        .config-preview strong {
          color: #172033;
        }

        .actions {
          display: flex;
          justify-content: space-between;
          gap: 12px;
          align-items: center;
        }

        .continue-actions {
          display: flex;
          gap: 12px;
        }

        .btn-back,
        .btn-force,
        .btn-continue {
          min-height: 42px;
          padding: 0 20px;
          border: 0;
          border-radius: 6px;
          cursor: pointer;
          font-weight: 800;
        }

        .btn-back {
          color: #334155;
          background: #e2e8f0;
        }

        .btn-force {
          color: #713f12;
          background: #fde68a;
        }

        .btn-continue {
          color: #fff;
          background: #0f766e;
        }

        .ai-mode-row {
          display: flex;
          align-items: center;
          gap: 14px;
        }

        .ai-toggle {
          display: flex;
          align-items: center;
          gap: 8px;
          cursor: pointer;
          color: #344054;
          font-size: 14px;
          font-weight: 700;
        }

        .ai-toggle input {
          width: 18px;
          height: 18px;
          accent-color: #7c3aed;
        }

        .ai-toggle input:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        button:disabled {
          opacity: 0.55;
          cursor: not-allowed;
        }

        @media (max-width: 720px) {
          .status-header,
          .actions {
            align-items: flex-start;
            flex-direction: column;
          }

          .config-preview {
            grid-template-columns: 1fr;
          }
        }
      `}</style>
    </div>
  );
}
