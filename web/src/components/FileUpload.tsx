import { useCallback, useRef, useState } from 'react';

interface FileUploadProps {
  onUpload: (file: File) => Promise<void>;
  onError: (error: string) => void;
  isLoading: boolean;
}

const ALLOWED_EXTENSIONS = ['.csv', '.xlsx', '.xls', '.parquet'];

function formatError(error: unknown) {
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  try {
    return JSON.stringify(error);
  } catch {
    return '上传失败，请稍后重试。';
  }
}

export function FileUpload({ onUpload, onError, isLoading }: FileUploadProps) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);

  const handleFile = useCallback(async (file: File) => {
    const ext = `.${file.name.split('.').pop()?.toLowerCase() || ''}`;

    if (!ALLOWED_EXTENSIONS.includes(ext)) {
      onError(`不支持 ${ext || '未知'} 格式。请上传 CSV、Excel 或 Parquet 文件。`);
      return;
    }

    setSelectedFile(file);
    try {
      await onUpload(file);
    } catch (error) {
      onError(formatError(error));
    }
  }, [onError, onUpload]);

  const handleDrag = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.stopPropagation();
    setDragActive(event.type === 'dragenter' || event.type === 'dragover');
  }, []);

  const handleDrop = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.stopPropagation();
    setDragActive(false);

    const file = event.dataTransfer.files?.[0];
    if (file) {
      void handleFile(file);
    }
  }, [handleFile]);

  const handleChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      void handleFile(file);
    }
  }, [handleFile]);

  return (
    <section className="file-upload-container" aria-label="上传数据文件">
      <div
        className={`upload-zone ${dragActive ? 'drag-active' : ''} ${isLoading ? 'loading' : ''}`}
        onDragEnter={handleDrag}
        onDragLeave={handleDrag}
        onDragOver={handleDrag}
        onDrop={handleDrop}
        onClick={() => inputRef.current?.click()}
        role="button"
        tabIndex={0}
        onKeyDown={(event) => {
          if (event.key === 'Enter' || event.key === ' ') inputRef.current?.click();
        }}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".csv,.xlsx,.xls,.parquet"
          onChange={handleChange}
          className="file-input"
        />

        {isLoading ? (
          <div className="upload-loading">
            <div className="spinner" />
            <h3>正在读取文件</h3>
            <p>系统正在解析列名、样例值和字段推荐。</p>
          </div>
        ) : (
          <>
            <div className="upload-icon" aria-hidden="true">↑</div>
            <h3>拖拽数据文件到这里</h3>
            <p>或点击选择文件，上传后会自动进入字段映射页面。</p>
            <button type="button" className="upload-button">
              选择文件
            </button>
            <p className="upload-hint">支持 CSV、Excel（.xlsx/.xls）、Parquet，建议首行保留清晰列名。</p>
          </>
        )}
      </div>

      {selectedFile && !isLoading && (
        <div className="selected-file">
          <span>已选择：{selectedFile.name}</span>
          <button
            type="button"
            onClick={(event) => {
              event.stopPropagation();
              setSelectedFile(null);
              if (inputRef.current) inputRef.current.value = '';
            }}
            aria-label="移除已选择文件"
          >
            ×
          </button>
        </div>
      )}

      <style>{`
        .file-upload-container {
          width: 100%;
        }

        .upload-zone {
          min-height: 310px;
          padding: 42px 24px;
          text-align: center;
          background: #fff;
          border: 2px dashed #b8c6d8;
          border-radius: 8px;
          cursor: pointer;
          transition: border-color 0.18s ease, background 0.18s ease, transform 0.18s ease;
          box-shadow: 0 16px 34px rgba(26, 38, 62, 0.08);
        }

        .upload-zone:hover,
        .upload-zone.drag-active {
          background: #f0fbf8;
          border-color: #0f766e;
          transform: translateY(-1px);
        }

        .upload-zone.loading {
          pointer-events: none;
        }

        .file-input {
          display: none;
        }

        .upload-icon {
          width: 56px;
          height: 56px;
          margin: 0 auto 18px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: #0f766e;
          background: #dff8f0;
          border-radius: 50%;
          font-size: 32px;
          font-weight: 800;
        }

        .upload-zone h3 {
          margin: 0 0 8px;
          color: #172033;
          font-size: 22px;
        }

        .upload-zone p {
          margin: 0 0 18px;
          color: #5d6b82;
          line-height: 1.7;
        }

        .upload-button {
          min-height: 42px;
          padding: 0 22px;
          color: #fff;
          background: #0f766e;
          border: 0;
          border-radius: 6px;
          cursor: pointer;
          font-weight: 700;
        }

        .upload-button:hover {
          background: #0b5f59;
        }

        .upload-hint {
          margin-top: 18px !important;
          font-size: 13px;
          color: #7a879a !important;
        }

        .upload-loading {
          padding: 34px 16px;
        }

        .spinner {
          width: 42px;
          height: 42px;
          border: 3px solid #dce6f2;
          border-top-color: #0f766e;
          border-radius: 50%;
          animation: spin 1s linear infinite;
          margin: 0 auto 16px;
        }

        @keyframes spin {
          to { transform: rotate(360deg); }
        }

        .selected-file {
          margin-top: 14px;
          padding: 12px 14px;
          color: #0f5132;
          background: #e8f7ef;
          border: 1px solid #b7e4cc;
          border-radius: 8px;
          display: flex;
          justify-content: space-between;
          align-items: center;
          gap: 12px;
        }

        .selected-file button {
          width: 30px;
          height: 30px;
          border: 0;
          color: #0f5132;
          background: transparent;
          cursor: pointer;
          font-size: 22px;
        }
      `}</style>
    </section>
  );
}
