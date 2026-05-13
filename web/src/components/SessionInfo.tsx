import { useEffect, useState } from 'react';

interface SessionInfoProps {
  sessionId: string;
  fileName?: string;
  totalRows?: number;
  fileSizeMb?: number;
  expiresAt?: string;
  onDestroy: () => void;
}

export function SessionInfo({
  sessionId,
  fileName,
  totalRows,
  fileSizeMb,
  expiresAt,
  onDestroy,
}: SessionInfoProps) {
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 30000);
    return () => window.clearInterval(timer);
  }, []);

  const remainingMinutes = expiresAt ? Math.floor((new Date(expiresAt).getTime() - now) / 60000) : undefined;

  const formatExpiresAt = (isoString?: string) => {
    if (!isoString) return '';
    const date = new Date(isoString);
    const diffMins = Math.floor((date.getTime() - now) / 60000);

    if (diffMins < 0) return '已过期';
    if (diffMins < 60) return `${diffMins} 分钟后过期`;
    return `约 ${Math.floor(diffMins / 60)} 小时后过期`;
  };

  return (
    <div className="session-info">
      <div className="session-badge">
        <div className="badge-icon" aria-hidden="true">S</div>
        <div className="badge-content">
          <span className="session-id">Session：{sessionId.slice(0, 8)}...</span>
          {fileName && <span className="file-name">{fileName}</span>}
          {totalRows && (
            <span className="file-meta">
              {totalRows.toLocaleString()} 行{fileSizeMb ? ` · ${fileSizeMb} MB` : ''}
            </span>
          )}
          {expiresAt && (
            <span className={`expires-at ${remainingMinutes !== undefined && remainingMinutes <= 10 ? 'warning' : ''}`}>
              {formatExpiresAt(expiresAt)}
            </span>
          )}
        </div>
        <button className="destroy-btn" onClick={onDestroy} title="清空当前数据">
          清空
        </button>
      </div>

      <style>{`
        .session-info {
          margin-bottom: 18px;
        }

        .session-badge {
          display: flex;
          align-items: center;
          gap: 12px;
          padding: 13px 16px;
          background: #ffffff;
          border: 1px solid #dfe6ef;
          border-radius: 8px;
          box-shadow: 0 10px 24px rgba(26, 38, 62, 0.05);
        }

        .badge-icon {
          width: 36px;
          height: 36px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: #0f766e;
          background: #dff8f0;
          border-radius: 6px;
          font-weight: 900;
        }

        .badge-content {
          flex: 1;
          min-width: 0;
          display: flex;
          flex-direction: column;
          gap: 2px;
        }

        .session-id {
          color: #667085;
          font-family: Consolas, "SFMono-Regular", monospace;
          font-size: 12px;
        }

        .file-name {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          color: #172033;
          font-weight: 800;
        }

        .file-meta,
        .expires-at {
          color: #667085;
          font-size: 13px;
        }

        .expires-at.warning {
          color: #b45309;
          font-weight: 800;
        }

        .destroy-btn {
          min-height: 36px;
          padding: 0 14px;
          color: #9f1239;
          background: #fff1f2;
          border: 1px solid #fecdd3;
          border-radius: 6px;
          cursor: pointer;
          font-weight: 800;
        }

        .destroy-btn:hover {
          color: #fff;
          background: #e11d48;
          border-color: #e11d48;
        }
      `}</style>
    </div>
  );
}
