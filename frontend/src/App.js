import React, { useState, useCallback } from "react";
import "./App.css";

const SERVICES = [
  {
    id: "pdf2png",
    title: "PDF -> PNG",
    description: "Конвертація PDF-документа у набір PNG-зображень.",
    color: "#4CAF50",
    accept: ".pdf,application/pdf",
    endpoint: "/pdf2png",
    filename: "pdf_pages.zip",
  },
  {
    id: "wav2mp3",
    title: "WAV -> MP3",
    description: "Конвертація аудіофайлу WAV у MP3.",
    color: "#2196F3",
    accept: ".wav,.wave,audio/wav",
    endpoint: "/file-request",
    filename: "result.mp3",
  },
  {
    id: "webp2png",
    title: "WEBP -> PNG",
    description: "Перетворення зображення WEBP у PNG.",
    color: "#FF9800",
    accept: ".webp,image/webp",
    endpoint: "/webp2png",
    filename: "image.png",
  },
  {
    id: "ziprar",
    title: "RAR -> ZIP",
    description: "Перепакування архiвiв RAR в ZIP",
    color: "#9C27B0",
    accept: ".zip,.rar,application/zip,application/x-rar-compressed",
    endpoint: "/ziprar",
    filename: "archive.zip",
  },
];

function App() {
  const [selectedService, setSelectedService] = useState(null);
  const [file, setFile] = useState(null);
  const [isUploading, setIsUploading] = useState(false);
  const [downloadUrl, setDownloadUrl] = useState(null);
  const [error, setError] = useState(null);
  const [algorithm, setAlgorithm] = useState("round_robin");

  // handlers (memoized to avoid useless re-renders)
  const handleTileClick = useCallback((service) => {
    setSelectedService(service);
    setFile(null);
    setDownloadUrl(null);
    setError(null);
  }, []);

  const handleFileChange = useCallback((e) => {
    const f = e.target.files?.[0] ?? null;
    setFile(f);
    setDownloadUrl(null);
    setError(null);
  }, []);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!selectedService || !file) return;

    setIsUploading(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("algorithm", algorithm);

      const response = await fetch(selectedService.endpoint, {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`Помилка сервера: ${response.status} ${text}`);
      }

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);

      // auto download
      const a = document.createElement("a");
      a.href = url;
      a.download = selectedService.filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);

      if (downloadUrl) URL.revokeObjectURL(downloadUrl);

      setDownloadUrl(url);
    } catch (err) {
      console.error(err);
      setError(err.message || "Невідома помилка");
    } finally {
      setIsUploading(false);
    }
  };

  return (
    <div className="page">
      <header className="header">
        <h1 className="title">File Conversion Platform</h1>
        <p className="subtitle">
          Оберіть сервіс конвертації, завантажте файл та отримайте результат.
        </p>
      </header>

      <main className="main">
        <section className="tilesSection">
          <h2 className="sectionTitle">Доступні сервіси</h2>

          <div className="tilesGrid">
            {SERVICES.map((service) => {
              const isActive =
                selectedService?.id === service.id;

              return (
                <button
                  key={service.id}
                  type="button"
                  className={`tile ${
                    isActive ? "active" : "inactive"
                  }`}
                  style={{ backgroundColor: service.color }}
                  onClick={() => handleTileClick(service)}
                >
                  <h3 className="tileTitle">{service.title}</h3>
                  <p className="tileDescription">
                    {service.description}
                  </p>
                </button>
              );
            })}
          </div>
        </section>

        {/* FORM */}
        <section className="formSection">
          <h2 className="sectionTitle">Панель завантаження</h2>

          {!selectedService && (
            <p className="hint">
              Спочатку оберіть сервіс конвертації, натиснувши на плитку.
            </p>
          )}

          {selectedService && (
            <form className="form" onSubmit={handleSubmit}>
              <div className="formRow">
                <label className="label">
                  Обраний сервіс:
                  <span className="selectedServiceName">
                    {selectedService.title}
                  </span>
                </label>
              </div>

              <div className="formRow">
                <label className="label">
                  Виберіть файл:
                  <input
                    type="file"
                    accept={selectedService.accept}
                    onChange={handleFileChange}
                    className="fileInput"
                  />
                </label>
              </div>

              <div className="formRow">
                <label className="label">
                  Алгоритм балансування:
                  <select
                    value={algorithm}
                    onChange={(e) => setAlgorithm(e.target.value)}
                    className="select"
                  >
                    <option value="round_robin">round_robin</option>
                    <option value="least_connections">
                      least_connections
                    </option>
                    <option value="ip_hash">ip_hash</option>
                    <option value="random">random</option>
                    <option value="power_of_two">power_of_two</option>
                  </select>
                </label>
              </div>

              <div className="formRow">
                <button
                  type="submit"
                  className="submitButton"
                  disabled={!file || isUploading}
                >
                  {isUploading
                    ? "Обробка..."
                    : "Надіслати на конвертацію"}
                </button>
              </div>

              {error && <p className="error">Помилка: {error}</p>}

              {downloadUrl && (
                <div className="formRow">
                  <a
                    href={downloadUrl}
                    download
                    className="downloadLink"
                  >
                    Завантажити оброблений файл
                  </a>
                </div>
              )}
            </form>
          )}
        </section>
      </main>
    </div>
  );
}

export default App;
