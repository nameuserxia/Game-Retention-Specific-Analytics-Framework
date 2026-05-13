/**
 * 游戏留存分析 Web 应用
 */

import { useState, useCallback } from 'react';
import { useAnalysis } from './hooks/useAnalysis';
import { FileUpload } from './components/FileUpload';
import { FieldMapper } from './components/FieldMapper';
import { ValidationResult } from './components/ValidationResult';
import { RetentionMatrix } from './components/RetentionMatrix';
import { SessionInfo } from './components/SessionInfo';
import type { FieldMappingRequest, AnalysisConfig } from './types/api';

export default function App() {
  const {
    state,
    isLoading,
    uploadFile,
    validateMapping,
    detectJsonKeys,
    runAnalysis,
    destroySession,
    reset,
    setError,
    clearError,
    updateState,
  } = useAnalysis();

  const [validationMapping, setValidationMapping] = useState<FieldMappingRequest | null>(null);
  const [validationConfig, setValidationConfig] = useState<AnalysisConfig | null>(null);
  const [llmText, setLlmText] = useState('');    // 实时 LLM 输出
  const [llmDone, setLlmDone] = useState(false); // LLM 流是否结束
  const [streamStatus, setStreamStatus] = useState(''); // 流式状态消息

  const analysisStages = [
    '读取 Parquet 数据',
    '应用字段映射',
    '数据质量校验',
    '计算留存与 Cohort',
    '分析流失路径',
    '运行 JSON/Agent 诊断',
    '生成 Markdown 报告',
  ];

  const handleUpload = useCallback(async (file: File) => {
    await uploadFile(file);
  }, [uploadFile]);

  const handleValidate = useCallback(async (mapping: FieldMappingRequest, config: AnalysisConfig) => {
    setValidationMapping(mapping);
    setValidationConfig(config);
    await validateMapping(mapping, false);
  }, [validateMapping]);

  const handleContinue = useCallback(async (forceProceed: boolean, useAiMode: boolean = false) => {
    if (!validationMapping || !validationConfig) return;

    setLlmText('');
    setLlmDone(false);
    setStreamStatus(useAiMode ? '正在生成 AI 结构化报告...' : '');
    await runAnalysis(
      validationMapping,
      { ...validationConfig, ai_enabled: useAiMode },
      forceProceed,
    );
    setLlmDone(useAiMode);
  }, [validationMapping, validationConfig, runAnalysis]);

  const handleBack = useCallback(() => {
    updateState({ step: 'upload', validation: undefined });
  }, [updateState]);

  const handleDestroy = useCallback(async () => {
    await destroySession();
    setValidationMapping(null);
    setValidationConfig(null);
  }, [destroySession]);

  const handleNewAnalysis = useCallback(() => {
    setValidationMapping(null);
    setValidationConfig(null);
    reset();
  }, [reset]);

  const handleBackToMapping = useCallback(() => {
    updateState({ step: 'upload', validation: undefined, result: undefined });
  }, [updateState]);

  const renderContent = () => {
    if (state.error) {
      return (
        <div className="error-banner" role="alert">
          <span>{state.error}</span>
          <button onClick={clearError} aria-label="关闭错误提示">×</button>
        </div>
      );
    }

    switch (state.step) {
      case 'idle':
        return (
          <div className="step-container">
            <div className="intro-panel">
              <div>
                <p className="eyebrow">从原始事件表到留存报告</p>
                <h2>上传数据，选择字段，自动计算游戏留存</h2>
                <p>
                  适合 CSV、Excel、Parquet 数据。上传后系统会读取列名并给出推荐映射，你只需要在下拉框里确认用户、事件时间、注册日期等字段。
                </p>
              </div>
              <div className="intro-steps" aria-label="分析流程">
                <span>1 上传数据</span>
                <span>2 勾选字段</span>
                <span>3 校验日期</span>
                <span>4 生成结果</span>
              </div>
            </div>
            <FileUpload onUpload={handleUpload} onError={setError} isLoading={isLoading} />
          </div>
        );

      case 'upload':
        return (
          <div className="step-container wide">
            {state.schema && (
              <>
                <SessionInfo
                  sessionId={state.schema.session_id}
                  fileName={state.schema.file_name}
                  totalRows={state.schema.total_rows}
                  fileSizeMb={state.schema.file_size_mb}
                  expiresAt={state.schema.expires_at}
                  onDestroy={handleDestroy}
                />
                <FieldMapper
                  schema={state.schema}
                  onValidate={handleValidate}
                  onDetectJsonKeys={detectJsonKeys}
                  isLoading={isLoading}
                />
              </>
            )}
          </div>
        );

      case 'mapping':
        if (!state.validation || !state.mapping || !validationConfig || !state.schema) return null;
        return (
          <div className="step-container wide">
            <SessionInfo
              sessionId={state.schema.session_id}
              fileName={state.schema.file_name}
              totalRows={state.schema.total_rows}
              fileSizeMb={state.schema.file_size_mb}
              expiresAt={state.schema.expires_at}
              onDestroy={handleDestroy}
            />
            <ValidationResult
              validation={state.validation}
              mapping={state.mapping}
              config={validationConfig}
              onContinue={handleContinue}
              onBack={handleBack}
              isLoading={isLoading}
            />
          </div>
        );

      case 'analyzing':
        return (
          <div className="loading-container">
            <div className="spinner-large" />
            <h3>正在分析数据</h3>
            {streamStatus ? (
              <p className="stream-status">{streamStatus}</p>
            ) : (
              <p>正在计算留存率、Cohort 矩阵和流失用户行为路径，请稍候。</p>
            )}
            {llmText ? (
              <div className="llm-preview">
                <h4>AI 报告生成中…</h4>
                <pre className="llm-live">{llmText}</pre>
              </div>
            ) : (
              <div className="analysis-stage-list">
                {analysisStages.map((stage, index) => (
                  <span key={stage} style={{ animationDelay: `${index * 0.35}s` }}>
                    {stage}
                  </span>
                ))}
              </div>
            )}
          </div>
        );

      case 'done':
        if (!state.result) return null;
        return (
          <div className="step-container wide">
            <RetentionMatrix
              result={state.result}
              onNewAnalysis={handleNewAnalysis}
              onBackToMapping={handleBackToMapping}
              onDestroySession={handleDestroy}
              llmText={llmText}
              llmDone={llmDone}
            />
          </div>
        );

      default:
        return null;
    }
  };

  const steps = [
    { key: 'idle', label: '上传文件' },
    { key: 'upload', label: '字段映射' },
    { key: 'mapping', label: '预校验' },
    { key: 'analyzing', label: '分析中' },
    { key: 'done', label: '完成' },
  ];

  const currentStepIndex = Math.max(steps.findIndex(s => s.key === state.step), 0);

  return (
    <div className="app">
      <header className="app-header">
        <div>
          <p className="header-kicker">Game Retention Framework</p>
          <h1>游戏留存分析框架</h1>
          <p>面向中文用户的数据上传、字段映射与留存分析工作台</p>
        </div>
      </header>

      {state.step !== 'idle' && (
        <nav className="progress-bar" aria-label="分析进度">
          {steps.slice(1).map((step, index) => {
            const realIndex = index + 1;
            return (
              <div
                key={step.key}
                className={`progress-step ${realIndex <= currentStepIndex ? 'active' : ''} ${realIndex < currentStepIndex ? 'completed' : ''}`}
              >
                <span className="step-number">{realIndex}</span>
                <span className="step-label">{step.label}</span>
              </div>
            );
          })}
        </nav>
      )}

      <main className="app-main">{renderContent()}</main>

      <footer className="app-footer">
        game_retention_framework v1.0
      </footer>

      <style>{`
        * {
          box-sizing: border-box;
        }

        body {
          margin: 0;
          color: #172033;
          background: #eef2f7;
          font-family: "Microsoft YaHei", "PingFang SC", "Hiragino Sans GB", "Noto Sans CJK SC", "Source Han Sans SC", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        }

        button,
        input,
        select,
        textarea {
          font: inherit;
        }

        .app {
          min-height: 100vh;
          display: flex;
          flex-direction: column;
        }

        .app-header {
          color: #fff;
          background: #152238;
          border-bottom: 1px solid rgba(255,255,255,0.14);
        }

        .app-header > div {
          max-width: 1160px;
          margin: 0 auto;
          padding: 28px 20px 30px;
        }

        .header-kicker,
        .eyebrow {
          margin: 0 0 8px;
          color: #65d6ad;
          font-size: 13px;
          font-weight: 700;
          letter-spacing: 0;
        }

        .app-header h1 {
          margin: 0 0 8px;
          font-size: 30px;
          line-height: 1.25;
        }

        .app-header p:last-child {
          margin: 0;
          color: #c7d2e2;
        }

        .progress-bar {
          display: flex;
          justify-content: center;
          gap: 10px;
          padding: 14px 16px;
          background: #fff;
          border-bottom: 1px solid #dfe6ef;
        }

        .progress-step {
          display: flex;
          align-items: center;
          gap: 8px;
          min-height: 38px;
          padding: 7px 14px;
          border: 1px solid #d7e0eb;
          border-radius: 8px;
          color: #6c7890;
          background: #f7f9fc;
        }

        .progress-step.active {
          color: #0f766e;
          border-color: #8dd8c6;
          background: #eefdf8;
        }

        .progress-step.completed {
          color: #1d4ed8;
          border-color: #b9cdfd;
          background: #eff5ff;
        }

        .step-number {
          width: 22px;
          height: 22px;
          border-radius: 50%;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          color: #fff;
          background: currentColor;
          font-size: 12px;
          font-weight: 700;
        }

        .step-label {
          font-size: 14px;
          font-weight: 700;
        }

        .app-main {
          flex: 1;
          padding: 28px 16px 36px;
        }

        .step-container {
          max-width: 860px;
          margin: 0 auto;
        }

        .step-container.wide {
          max-width: 1120px;
        }

        .intro-panel {
          display: grid;
          grid-template-columns: 1fr 280px;
          gap: 24px;
          align-items: stretch;
          margin-bottom: 22px;
          padding: 26px;
          color: #172033;
          background: #fff;
          border: 1px solid #dfe6ef;
          border-radius: 8px;
          box-shadow: 0 16px 34px rgba(26, 38, 62, 0.08);
        }

        .intro-panel h2 {
          margin: 0 0 12px;
          font-size: 28px;
          line-height: 1.3;
        }

        .intro-panel p:last-child {
          margin: 0;
          color: #5d6b82;
          line-height: 1.8;
        }

        .intro-steps {
          display: grid;
          gap: 10px;
          align-content: center;
          padding-left: 20px;
          border-left: 1px solid #dfe6ef;
        }

        .intro-steps span {
          padding: 10px 12px;
          background: #f5f8fc;
          border: 1px solid #dfe6ef;
          border-radius: 6px;
          font-weight: 700;
        }

        .error-banner {
          max-width: 860px;
          margin: 0 auto 20px;
          padding: 14px 16px;
          color: #8a1c25;
          background: #fff1f2;
          border: 1px solid #fecdd3;
          border-radius: 8px;
          display: flex;
          justify-content: space-between;
          align-items: center;
          gap: 16px;
        }

        .error-banner button {
          width: 32px;
          height: 32px;
          border: 0;
          color: #8a1c25;
          background: transparent;
          cursor: pointer;
          font-size: 22px;
        }

        .loading-container {
          max-width: 620px;
          margin: 0 auto;
          text-align: center;
          padding: 64px 20px;
          background: #fff;
          border: 1px solid #dfe6ef;
          border-radius: 8px;
        }

        .spinner-large {
          width: 58px;
          height: 58px;
          border: 4px solid #dce6f2;
          border-top-color: #0f766e;
          border-radius: 50%;
          animation: spin 1s linear infinite;
          margin: 0 auto 22px;
        }

        @keyframes spin {
          to { transform: rotate(360deg); }
        }

        .loading-container h3 {
          margin: 0 0 8px;
          font-size: 22px;
        }

        .loading-container p {
          margin: 0;
          color: #5d6b82;
        }

        .stream-status {
          color: #7c3aed !important;
          font-weight: 700;
          font-size: 15px;
        }

        .llm-preview {
          margin-top: 22px;
          padding: 16px;
          background: #faf9ff;
          border: 1px solid #ddd6fe;
          border-radius: 8px;
          text-align: left;
        }

        .llm-preview h4 {
          margin: 0 0 10px;
          color: #7c3aed;
          font-size: 14px;
        }

        .llm-live {
          margin: 0;
          padding: 12px;
          max-height: 380px;
          overflow-y: auto;
          background: #fff;
          border: 1px solid #e2e8f0;
          border-radius: 6px;
          color: #172033;
          font-size: 13px;
          line-height: 1.6;
          white-space: pre-wrap;
          word-break: break-word;
        }
          margin: 0;
          color: #5d6b82;
        }

        .analysis-stage-list {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          gap: 8px;
          margin-top: 22px;
          text-align: left;
        }

        .analysis-stage-list span {
          padding: 9px 10px;
          color: #0f766e;
          background: #e8f7ef;
          border: 1px solid #b7e4cc;
          border-radius: 6px;
          font-size: 13px;
          font-weight: 800;
          animation: pulseStage 2.8s ease-in-out infinite;
        }

        @keyframes pulseStage {
          0%, 100% { opacity: 0.55; }
          45% { opacity: 1; }
        }

        .app-footer {
          padding: 18px;
          text-align: center;
          color: #6c7890;
          font-size: 13px;
        }

        @media (max-width: 760px) {
          .app-header > div {
            padding: 22px 16px;
          }

          .intro-panel {
            grid-template-columns: 1fr;
            padding: 20px;
          }

          .intro-steps {
            padding-left: 0;
            border-left: 0;
          }

          .progress-bar {
            justify-content: flex-start;
            overflow-x: auto;
          }
        }
      `}</style>
    </div>
  );
}
