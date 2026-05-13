import { useState } from 'react';
import type { AnalysisResponse } from '../types/api';

interface RetentionMatrixProps {
  result: AnalysisResponse;
  onNewAnalysis: () => void;
  onBackToMapping: () => void;
  onDestroySession: () => void;
  /** AI 流式模式的实时 Markdown 文本 */
  llmText?: string;
  /** AI 流式模式是否已完成 */
  llmDone?: boolean;
}

export function RetentionMatrix({
  result,
  onNewAnalysis,
  onBackToMapping,
  onDestroySession,
  llmText = '',
  llmDone = false,
}: RetentionMatrixProps) {
  const [showAllCohorts, setShowAllCohorts] = useState(false);
  const {
    summary,
    cohort_headers,
    cohort_matrix,
    country_retention,
    channel_retention,
    top_paths,
    report_markdown,
    diagnostics,
    virtual_fields,
    dynamic_retention = [],
    funnel_analysis,
  } = result;

  const structuredDiagnosis = (diagnostics?.structured_diagnosis || {}) as Record<string, string>;
  const agentDiagnosis = (diagnostics?.agent_diagnosis || {}) as Record<string, unknown>;
  const agentReport = (agentDiagnosis.structured_report || {}) as Record<string, string>;
  const visibleCohortRows = showAllCohorts ? cohort_matrix : cohort_matrix.slice(0, 12);

  const getRetentionColor = (rate: number) => {
    if (rate >= 50) return '#166534';
    if (rate >= 30) return '#15803d';
    if (rate >= 20) return '#16a34a';
    if (rate >= 10) return '#65a30d';
    if (rate >= 5) return '#ca8a04';
    if (rate >= 1) return '#f59e0b';
    return '#e11d48';
  };

  const formatRetention = (rate: unknown) => {
    const num = typeof rate === 'number' ? rate : parseFloat(String(rate));
    return Number.isNaN(num) ? '-' : `${num.toFixed(1)}%`;
  };

  const renderSegmentTable = (
    title: string,
    firstColumn: string,
    rows: typeof country_retention,
  ) => {
    if (!rows.length) return null;

    return (
      <section className="panel">
        <div className="section-title">
          <h3>{title}</h3>
          <span>展示前 {Math.min(rows.length, 10)} 项</span>
        </div>
        <div className="table-wrapper">
          <table className="segment-table">
            <thead>
              <tr>
                <th>{firstColumn}</th>
                <th>总用户数</th>
                <th>留存用户</th>
                <th>留存率</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 10).map(row => (
                <tr key={row.segment}>
                  <td>{row.segment}</td>
                  <td>{row.n_total.toLocaleString()}</td>
                  <td>{row.n_retained.toLocaleString()}</td>
                  <td>
                    <span className="retention-badge" style={{ backgroundColor: getRetentionColor(row.retention_rate) }}>
                      {row.retention_rate.toFixed(1)}%
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    );
  };

  return (
    <div className="analysis-results">
      <section className="summary-header">
        <div>
          <p className="panel-kicker">分析完成</p>
          <h2>D+{summary.retention_days} 留存率：{summary.retention_rate.toFixed(1)}%</h2>
          <p>注册窗口：{summary.reg_start} 至 {summary.reg_end}</p>
        </div>
        <div className="summary-cards">
          <div className="summary-card">
            <span>总用户数</span>
            <strong>{summary.n_total.toLocaleString()}</strong>
          </div>
          <div className="summary-card highlight">
            <span>留存用户</span>
            <strong>{summary.n_retained.toLocaleString()}</strong>
          </div>
          <div className="summary-card danger">
            <span>流失用户</span>
            <strong>{summary.n_churn.toLocaleString()}</strong>
          </div>
          <div className="summary-card">
            <span>留存率</span>
            <strong>{summary.retention_rate.toFixed(1)}%</strong>
          </div>
        </div>
      </section>

      {cohort_matrix.length > 0 && (
        <section className="panel">
          <div className="section-title">
            <h3>Cohort 留存矩阵</h3>
            <span>显示 {visibleCohortRows.length}/{cohort_matrix.length} 个 cohort</span>
          </div>
          <div className="cohort-legend" aria-label="留存率颜色图例">
            {[1, 5, 10, 20, 30, 50].map(rate => (
              <span key={rate}>
                <i style={{ backgroundColor: getRetentionColor(rate) }} />
                ≥{rate}%
              </span>
            ))}
          </div>
          <div className="table-wrapper">
            <table className="cohort-table">
              <thead>
                <tr>
                  {cohort_headers.map((header, index) => <th key={index}>{header}</th>)}
                </tr>
              </thead>
              <tbody>
                {visibleCohortRows.map((row, rowIndex) => (
                  <tr key={rowIndex}>
                    {row.map((cell, columnIndex) => {
                      const rate = parseFloat(String(cell));
                      const isHeader = columnIndex === 0;
                      return (
                        <td
                          key={columnIndex}
                          style={{
                            backgroundColor: isHeader ? '#f8fafc' : getRetentionColor(rate),
                            color: isHeader || rate < 10 ? '#172033' : '#fff',
                          }}
                        >
                          {isHeader ? String(cell) : formatRetention(cell)}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {cohort_matrix.length > 12 && (
            <button className="toggle-cohorts-btn" type="button" onClick={() => setShowAllCohorts(prev => !prev)}>
              {showAllCohorts ? '收起 Cohort' : `展开全部 ${cohort_matrix.length} 个 Cohort`}
            </button>
          )}
        </section>
      )}

      {renderSegmentTable('按国家/地区分群', '国家/地区', country_retention)}
      {renderSegmentTable('按渠道来源分群', '渠道', channel_retention)}

      {dynamic_retention.length > 0 && (
        <section className="panel">
          <div className="section-title">
            <h3>动态分群留存分析</h3>
            <span>{dynamic_retention.length} 个维度组合</span>
          </div>
          {dynamic_retention.slice(0, 3).map(item => (
            <div className="dynamic-block" key={item.dimensions.join('|') || 'dynamic'}>
              <h4>{item.dimensions.join(' + ') || '未命名维度'}</h4>
              {item.warnings?.length > 0 && <p className="inline-warning">{item.warnings.join('；')}</p>}
              {item.groups.length > 0 ? (
                <div className="table-wrapper">
                  <table className="segment-table">
                    <thead>
                      <tr>
                        <th>分组</th>
                        <th>样本量</th>
                        <th>D1</th>
                        <th>D3</th>
                        <th>D7</th>
                        <th>D14</th>
                        <th>提示</th>
                      </tr>
                    </thead>
                    <tbody>
                      {item.groups.slice(0, 10).map(group => (
                        <tr key={group.group_key}>
                          <td>{group.group_key}</td>
                          <td>{group.cohort_size.toLocaleString()}</td>
                          <td>{formatRetention((group.retention.D1 ?? 0) * 100)}</td>
                          <td>{formatRetention((group.retention.D3 ?? 0) * 100)}</td>
                          <td>{formatRetention((group.retention.D7 ?? 0) * 100)}</td>
                          <td>{formatRetention((group.retention.D14 ?? 0) * 100)}</td>
                          <td>{group.sample_warning ? '样本过小' : ''}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="inline-warning">当前维度组合没有可展示分组。</p>
              )}
            </div>
          ))}
        </section>
      )}

      {funnel_analysis?.steps?.length ? (
        <section className="panel">
          <div className="section-title">
            <h3>漏斗转化分析</h3>
            <span>{funnel_analysis.steps.length} 个步骤</span>
          </div>
          {funnel_analysis.warnings?.length > 0 && <p className="inline-warning">{funnel_analysis.warnings.join('；')}</p>}
          <div className="table-wrapper">
            <table className="segment-table">
              <thead>
                <tr>
                  <th>步骤</th>
                  <th>用户数</th>
                  <th>单步转化</th>
                  <th>总体转化</th>
                  <th>流失用户</th>
                  <th>流失率</th>
                </tr>
              </thead>
              <tbody>
                {funnel_analysis.steps.map((step, index) => (
                  <tr key={`${step.event}-${index}`}>
                    <td>{step.event}</td>
                    <td>{step.users.toLocaleString()}</td>
                    <td>{formatRetention(step.step_conversion_rate * 100)}</td>
                    <td>{formatRetention(step.overall_conversion_rate * 100)}</td>
                    <td>{step.dropoff_users.toLocaleString()}</td>
                    <td>{formatRetention(step.dropoff_rate * 100)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {structuredDiagnosis && Object.keys(structuredDiagnosis).length > 0 && (
        <section className="panel diagnosis-panel">
          <div className="section-title">
            <h3>四阶段自动诊断</h3>
            <span>{virtual_fields?.length ? `已生成 ${virtual_fields.length} 个虚拟字段` : '未生成虚拟字段'}</span>
          </div>
          <div className="diagnosis-grid">
            <div>
              <span>现象</span>
              <strong>{structuredDiagnosis.phenomenon || '暂无明显异常'}</strong>
            </div>
            <div>
              <span>归因</span>
              <strong>{structuredDiagnosis.attribution || '暂无明确归因'}</strong>
            </div>
            <div>
              <span>建议</span>
              <strong>{structuredDiagnosis.suggestion || '继续观察分群和关键路径变化'}</strong>
            </div>
          </div>
          {Object.keys(agentReport).length > 0 && (
            <div className="agent-report">
              <h4>Agent 诊断书</h4>
              <dl>
                <dt>数据体检</dt>
                <dd>{agentReport.data_checkup}</dd>
                <dt>异动定位</dt>
                <dd>{agentReport.anomaly_location}</dd>
                <dt>核心归因</dt>
                <dd>{agentReport.core_attribution}</dd>
                <dt>业务策略</dt>
                <dd>{agentReport.business_strategy}</dd>
              </dl>
            </div>
          )}
        </section>
      )}

      {/* ── AI 流式分析报告（LLM 模式） ─────────────────────── */}
      {llmText && (
        <section className="panel llm-report-panel">
          <div className="section-title">
            <h3>AI 专家分析报告</h3>
            {!llmDone && <span className="llm-badge">生成中…</span>}
            {llmDone && <span className="llm-badge-done">已完成</span>}
          </div>
          <div className="llm-markdown-body">
            <pre className="llm-output">{llmText}</pre>
          </div>
        </section>
      )}

      {top_paths.length > 0 && (
        <section className="panel">
          <div className="section-title">
            <h3>流失用户典型行为路径</h3>
            <span>最多展示前 5 步</span>
          </div>
          <div className="table-wrapper">
            <table className="paths-table">
              <thead>
                <tr>
                  <th>排名</th>
                  <th>行为路径</th>
                  <th>用户数</th>
                  <th>占比</th>
                </tr>
              </thead>
              <tbody>
                {top_paths.map(path => {
                  const steps = path.path.split(' → ');
                  return (
                    <tr key={path.rank}>
                      <td>{path.rank}</td>
                      <td className="path-cell">
                        {steps.map((step, index) => (
                          <span key={`${step}-${index}`} className="path-step">
                            {step}
                            {index < steps.length - 1 && <span className="arrow">→</span>}
                          </span>
                        ))}
                      </td>
                      <td>{path.count.toLocaleString()}</td>
                      <td>{path.pct}%</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <section className="panel report-panel">
        <div>
          <h3>分析报告</h3>
          <p>可以下载 Markdown 报告，也可以清空当前上传数据后重新开始。</p>
        </div>
        <div className="report-actions">
          <a
            href={`data:text/markdown;charset=utf-8,${encodeURIComponent(report_markdown)}`}
            download={`retention_report_${result.session_id.slice(0, 8)}.md`}
            className="download-btn"
          >
            下载报告
          </a>
          <button className="destroy-btn" onClick={onDestroySession}>
            清空数据
          </button>
        </div>
      </section>

      <div className="actions">
        <button className="edit-mapping-btn" onClick={onBackToMapping}>
          返回修改配置
        </button>
        <button className="new-analysis-btn" onClick={onNewAnalysis}>
          开始新的分析
        </button>
      </div>

      <style>{`
        .analysis-results {
          display: grid;
          gap: 18px;
        }

        .summary-header,
        .panel {
          background: #fff;
          border: 1px solid #dfe6ef;
          border-radius: 8px;
          box-shadow: 0 12px 28px rgba(26, 38, 62, 0.06);
        }

        .summary-header {
          display: grid;
          grid-template-columns: minmax(260px, 0.9fr) minmax(520px, 1.1fr);
          gap: 22px;
          align-items: center;
          padding: 24px;
        }

        .panel-kicker {
          margin: 0 0 8px;
          color: #0f766e;
          font-size: 13px;
          font-weight: 900;
        }

        .summary-header h2 {
          margin: 0 0 8px;
          color: #172033;
          font-size: 28px;
          line-height: 1.3;
        }

        .summary-header p {
          margin: 0;
          color: #667085;
        }

        .summary-cards {
          display: grid;
          grid-template-columns: repeat(4, minmax(0, 1fr));
          gap: 12px;
        }

        .summary-card {
          min-height: 104px;
          padding: 15px;
          background: #f8fafc;
          border: 1px solid #e2e8f0;
          border-radius: 8px;
        }

        .summary-card.highlight {
          color: #0f5132;
          background: #e8f7ef;
          border-color: #b7e4cc;
        }

        .summary-card.danger {
          color: #9f1239;
          background: #fff1f2;
          border-color: #fecdd3;
        }

        .summary-card span {
          display: block;
          margin-bottom: 14px;
          color: inherit;
          opacity: 0.76;
          font-size: 13px;
        }

        .summary-card strong {
          display: block;
          font-size: 28px;
          line-height: 1.1;
        }

        .panel {
          padding: 22px;
        }

        .section-title {
          display: flex;
          justify-content: space-between;
          gap: 12px;
          align-items: center;
          margin-bottom: 14px;
        }

        .section-title h3,
        .report-panel h3 {
          margin: 0;
          color: #172033;
          font-size: 18px;
        }

        .section-title span,
        .report-panel p {
          color: #667085;
          font-size: 13px;
        }

        .table-wrapper {
          overflow-x: auto;
        }

        .cohort-legend {
          display: flex;
          flex-wrap: wrap;
          gap: 10px;
          margin: 0 0 12px;
          color: #667085;
          font-size: 12px;
        }

        .cohort-legend span {
          display: inline-flex;
          align-items: center;
          gap: 5px;
        }

        .cohort-legend i {
          width: 18px;
          height: 12px;
          border-radius: 3px;
          display: inline-block;
        }

        .cohort-table,
        .segment-table,
        .paths-table {
          width: 100%;
          border-collapse: collapse;
        }

        th,
        td {
          padding: 11px 12px;
          border: 1px solid #e8edf4;
          text-align: center;
        }

        th {
          color: #475467;
          background: #f8fafc;
          font-size: 13px;
        }

        .cohort-table td {
          min-width: 70px;
          font-weight: 800;
        }

        .segment-table td:first-child,
        .paths-table td:nth-child(2) {
          text-align: left;
        }

        .retention-badge {
          display: inline-flex;
          min-width: 68px;
          justify-content: center;
          padding: 4px 10px;
          color: #fff;
          border-radius: 999px;
          font-weight: 800;
        }

        .dynamic-block {
          margin-top: 16px;
        }

        .dynamic-block:first-of-type {
          margin-top: 0;
        }

        .dynamic-block h4 {
          margin: 0 0 10px;
          color: #172033;
          font-size: 15px;
        }

        .inline-warning {
          margin: 0 0 10px;
          padding: 8px 10px;
          color: #854d0e;
          background: #fffbeb;
          border: 1px solid #fde68a;
          border-radius: 6px;
          font-size: 13px;
        }

        .path-cell {
          min-width: 320px;
        }

        .path-step {
          display: inline-flex;
          align-items: center;
          gap: 5px;
          margin-right: 6px;
        }

        .arrow {
          color: #98a2b3;
        }

        .report-panel {
          display: flex;
          justify-content: space-between;
          gap: 18px;
          align-items: center;
        }

        .report-panel p {
          margin: 8px 0 0;
        }

        .report-actions {
          display: flex;
          gap: 10px;
          white-space: nowrap;
        }

        .diagnosis-grid {
          display: grid;
          grid-template-columns: repeat(3, minmax(0, 1fr));
          gap: 12px;
        }

        .diagnosis-grid div {
          padding: 14px;
          background: #f8fafc;
          border: 1px solid #e2e8f0;
          border-radius: 8px;
        }

        .diagnosis-grid span {
          display: block;
          margin-bottom: 8px;
          color: #0f766e;
          font-size: 13px;
          font-weight: 900;
        }

        .diagnosis-grid strong {
          color: #172033;
          line-height: 1.7;
          font-size: 14px;
        }

        .ml-diagnosis {
          display: grid;
          grid-template-columns: minmax(0, 1fr) minmax(220px, 0.7fr);
          gap: 12px;
          margin-top: 12px;
          padding: 14px;
          background: #f0fbf8;
          border: 1px solid #b7e4cc;
          border-radius: 8px;
        }

        .ml-diagnosis span,
        .ml-diagnosis small {
          display: block;
        }

        .ml-diagnosis span {
          margin-bottom: 6px;
          color: #0f766e;
          font-weight: 900;
          font-size: 13px;
        }

        .ml-diagnosis strong {
          display: block;
          color: #172033;
          line-height: 1.7;
        }

        .ml-diagnosis small {
          margin-top: 6px;
          color: #667085;
        }

        .ml-diagnosis ol {
          margin: 0;
          padding-left: 20px;
          color: #344054;
        }

        .ml-diagnosis li {
          margin: 4px 0;
        }

        .agent-report {
          margin-top: 12px;
          padding: 14px;
          background: #fff;
          border: 1px solid #e2e8f0;
          border-radius: 8px;
        }

        .agent-report h4 {
          margin: 0 0 12px;
          color: #172033;
        }

        .agent-report dl {
          display: grid;
          grid-template-columns: 92px 1fr;
          gap: 8px 12px;
          margin: 0;
        }

        .agent-report dt {
          color: #0f766e;
          font-weight: 900;
        }

        .agent-report dd {
          margin: 0;
          color: #344054;
          line-height: 1.7;
        }

        .llm-report-panel {
          border-color: #ddd6fe;
          background: #faf9ff;
        }

        .llm-badge {
          display: inline-flex;
          align-items: center;
          padding: 3px 10px;
          color: #fff;
          background: #7c3aed;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 800;
          animation: llm-pulse 1.4s ease-in-out infinite;
        }

        @keyframes llm-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.6; }
        }

        .llm-badge-done {
          display: inline-flex;
          align-items: center;
          padding: 3px 10px;
          color: #fff;
          background: #059669;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 800;
        }

        .llm-markdown-body {
          margin-top: 12px;
          padding: 16px;
          background: #fff;
          border: 1px solid #e2e8f0;
          border-radius: 8px;
        }

        .llm-output {
          margin: 0;
          color: #172033;
          font-size: 14px;
          line-height: 1.8;
          white-space: pre-wrap;
          word-break: break-word;
          font-family: inherit;
        }

        .download-btn,
        .destroy-btn,
        .edit-mapping-btn,
        .toggle-cohorts-btn,
        .new-analysis-btn {
          min-height: 42px;
          padding: 0 18px;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          border: 0;
          border-radius: 6px;
          cursor: pointer;
          font-weight: 800;
          text-decoration: none;
        }

        .download-btn,
        .new-analysis-btn {
          color: #fff;
          background: #0f766e;
        }

        .edit-mapping-btn {
          color: #0f766e;
          background: #e8f7ef;
          border: 1px solid #b7e4cc;
        }

        .destroy-btn {
          color: #9f1239;
          background: #fff1f2;
          border: 1px solid #fecdd3;
        }

        .toggle-cohorts-btn {
          margin-top: 12px;
          color: #0f766e;
          background: #e8f7ef;
          border: 1px solid #b7e4cc;
        }

        .actions {
          display: flex;
          gap: 10px;
          justify-content: center;
          padding: 6px 0 12px;
        }

        @media (max-width: 900px) {
          .summary-header,
          .report-panel {
            grid-template-columns: 1fr;
            flex-direction: column;
            align-items: stretch;
          }

          .summary-cards {
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }

          .diagnosis-grid {
            grid-template-columns: 1fr;
          }

          .ml-diagnosis {
            grid-template-columns: 1fr;
          }

          .agent-report dl {
            grid-template-columns: 1fr;
          }
        }

        @media (max-width: 560px) {
          .summary-cards {
            grid-template-columns: 1fr;
          }

          .report-actions {
            flex-direction: column;
          }
        }
      `}</style>
    </div>
  );
}
