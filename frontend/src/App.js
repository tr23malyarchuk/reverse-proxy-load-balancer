import React, {
    useState
} from "react";

const SERVICES = [{
        id: "pdf2png",
        title: "PDF → PNG",
        description: "Конвертація PDF-документа у набір PNG-зображень.",
        color: "#4CAF50",
        accept: ".pdf,application/pdf",
    },
    {
        id: "wav2mp3",
        title: "WAV → MP3",
        description: "Конвертація аудіофайлу WAV у MP3.",
        color: "#2196F3",
        accept: ".wav,.wave,audio/wav",
    },
    {
        id: "webp2png",
        title: "WEBP → PNG",
        description: "Перетворення зображення WEBP у PNG.",
        color: "#FF9800",
        accept: ".webp,image/webp",
    },
    {
        id: "ziprar",
        title: "ZIP ↔ RAR",
        description: "Перепакування архівів ZIP та RAR.",
        color: "#9C27B0",
        accept: ".zip,.rar,application/zip,application/x-rar-compressed",
    },
];

function App() {
    const [selectedService, setSelectedService] = useState(null);
    const [file, setFile] = useState(null);
    const [isUploading, setIsUploading] = useState(false);
    const [downloadUrl, setDownloadUrl] = useState(null);
    const [error, setError] = useState(null);
    const [algorithm, setAlgorithm] = useState("round_robin");

    const handleTileClick = (service) => {
        setSelectedService(service);
        setFile(null);
        setDownloadUrl(null);
        setError(null);
    };

    const handleFileChange = (e) => {
        const f = e.target.files?.[0] ?? null;
        setFile(f);
        setDownloadUrl(null);
        setError(null);
    };

    const buildEndpoint = (serviceId) => {
        switch (serviceId) {
            case "wav2mp3":
                return "/file-request";
            case "pdf2png":
                return "/pdf2png";
            case "webp2png":
                return "/webp2png";
            case "ziprar":
                return "/ziprar";
            default:
                return "/file-request";
        }
    };

    const getSuggestedFilename = (serviceId) => {
        switch (serviceId) {
            case "wav2mp3":
                return "result.mp3";
            case "pdf2png":
                return "pdf_pages.zip";
            case "webp2png":
                return "image.png";
            case "ziprar":
                return "archive.zip";
            default:
                return "result.bin";
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        if (!selectedService || !file) return;

        setIsUploading(true);
        setError(null);
        setDownloadUrl(null);

        try {
            const formData = new FormData();
            formData.append("file", file);
            formData.append("algorithm", algorithm);

            const endpoint = buildEndpoint(selectedService.id);

            const response = await fetch(endpoint, {
                method: "POST",
                body: formData,
            });

            if (!response.ok) {
                const text = await response.text();
                throw new Error(`Помилка сервера: ${response.status} ${text}`);
            }

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);

            const suggestedName = getSuggestedFilename(selectedService.id);

            const a = document.createElement("a");
            a.href = url;
            a.download = suggestedName;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);

            setDownloadUrl(url);
        } catch (err) {
            console.error(err);
            setError(err.message || "Невідома помилка");
        } finally {
            setIsUploading(false);
        }
    };

    return ( <
        div style = {
            styles.page
        } >
        <
        header style = {
            styles.header
        } >
        <
        h1 style = {
            styles.title
        } > File Conversion Platform < /h1> <
        p style = {
            styles.subtitle
        } >
        Оберіть сервіс конвертації, завантажте файл та отримайте результат. <
        /p> <
        /header>

        <
        main style = {
            styles.main
        } >
        <
        section style = {
            styles.tilesSection
        } >
        <
        h2 style = {
            styles.sectionTitle
        } > Доступні сервіси < /h2> <
        div style = {
            styles.tilesGrid
        } > {
            SERVICES.map((service) => ( <
                button key = {
                    service.id
                }
                style = {
                    {
                        ...styles.tile,
                        backgroundColor: service.color,
                        border: selectedService && selectedService.id === service.id ?
                            "3px solid #fff" :
                            "2px solid rgba(255,255,255,0.5)",
                    }
                }
                onClick = {
                    () => handleTileClick(service)
                }
                type = "button" >
                <
                h3 style = {
                    styles.tileTitle
                } > {
                    service.title
                } < /h3> <
                p style = {
                    styles.tileDescription
                } > {
                    service.description
                } < /p> <
                /button>
            ))
        } <
        /div> <
        /section>

        <
        section style = {
            styles.formSection
        } >
        <
        h2 style = {
            styles.sectionTitle
        } > Панель завантаження < /h2> {
            !selectedService && ( <
                p style = {
                    styles.hint
                } >
                Спочатку оберіть сервіс конвертації, натиснувши на плитку. <
                /p>
            )
        }

        {
            selectedService && ( <
                    form style = {
                        styles.form
                    }
                    onSubmit = {
                        handleSubmit
                    } >
                    <
                    div style = {
                        styles.formRow
                    } >
                    <
                    label style = {
                        styles.label
                    } >
                    Обраний сервіс:
                    <
                    span style = {
                        styles.selectedServiceName
                    } > {
                        selectedService.title
                    } <
                    /span> <
                    /label> <
                    /div>

                    <
                    div style = {
                        styles.formRow
                    } >
                    <
                    label style = {
                        styles.label
                    } >
                    Виберіть файл:
                    <
                    input type = "file"
                    accept = {
                        selectedService.accept
                    }
                    onChange = {
                        handleFileChange
                    }
                    style = {
                        styles.fileInput
                    }
                    /> <
                    /label> <
                    /div>

                    <
                    div style = {
                        styles.formRow
                    } >
                    <
                    label style = {
                        styles.label
                    } >
                    Алгоритм балансування:
                    <
                    select value = {
                        algorithm
                    }
                    onChange = {
                        (e) => setAlgorithm(e.target.value)
                    }
                    style = {
                        {
                            marginLeft: 8,
                            padding: 4
                        }
                    } >
                    <
                    option value = "round_robin" > round_robin < /option> <
                    option value = "least_connections" > least_connections < /option> <
                    option value = "ip_hash" > ip_hash < /option> <
                    option value = "random" > random < /option> <
                    option value = "power_of_two" > power_of_two < /option> <
                    /select> <
                    /label> <
                    /div>

                    <
                    div style = {
                        styles.formRow
                    } >
                    <
                    button type = "submit"
                    style = {
                        styles.submitButton
                    }
                    disabled = {
                        !file || isUploading
                    } >
                    {
                        isUploading ? "Обробка..." : "Надіслати на конвертацію"
                    } <
                    /button> <
                    /div>

                    {
                        error && < p style = {
                            styles.error
                        } > Помилка: {
                            error
                        } < /p>}

                        {
                            downloadUrl && ( <
                                div style = {
                                    styles.formRow
                                } >
                                <
                                a href = {
                                    downloadUrl
                                }
                                download style = {
                                    styles.downloadLink
                                } >
                                Завантажити оброблений файл <
                                /a> <
                                /div>
                            )
                        } <
                        /form>

                    )
                } <
                /section> <
                /main>

                <
                footer style = {
                    styles.footer
                } >
                <
                p style = {
                    styles.footerText
                } >
                Reverse proxy load - balancing system· дипломний проєкт <
                /p> <
                /footer> <
                /div>
        );
    }

    const styles = {
        page: {
            minHeight: "100vh",
            margin: 0,
            padding: "0 16px 24px",
            fontFamily: "system-ui, -apple-system, BlinkMacSystemFont, sans-serif",
            background: "linear-gradient(135deg, #0f172a 0%, #020617 50%, #111827 100%)",
            color: "#e5e7eb",
            display: "flex",
            flexDirection: "column",
            boxSizing: "border-box",
        },
        header: {
            textAlign: "center",
            padding: "24px 0 16px",
        },
        title: {
            margin: 0,
            fontSize: "28px",
            color: "#f9fafb",
        },
        subtitle: {
            marginTop: "8px",
            color: "#9ca3af",
            fontSize: "14px",
        },
        main: {
            display: "flex",
            flexWrap: "wrap",
            gap: "24px",
            justifyContent: "center",
            maxWidth: "1100px",
            width: "100%",
            margin: "0 auto",
        },
        tilesSection: {
            flex: "1 1 320px",
            maxWidth: "600px",
        },
        formSection: {
            flex: "1 1 280px",
            maxWidth: "420px",
            backgroundColor: "rgba(15, 23, 42, 0.85)",
            borderRadius: "16px",
            padding: "16px 20px 20px",
            border: "1px solid rgba(148, 163, 184, 0.4)",
        },
        sectionTitle: {
            margin: "0 0 12px",
            fontSize: "18px",
            color: "#e5e7eb",
        },
        tilesGrid: {
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
            gap: "14px",
        },
        tile: {
            cursor: "pointer",
            borderRadius: "16px",
            padding: "14px 12px",
            textAlign: "left",
            color: "#f9fafb",
            boxShadow: "0 10px 25px rgba(0,0,0,0.35)",
            transition: "transform 0.12s ease, box-shadow 0.12s ease, opacity 0.12s",
            opacity: 0.95,
        },
        tileTitle: {
            margin: "0 0 6px",
            fontSize: "16px",
        },
        tileDescription: {
            margin: 0,
            fontSize: "13px",
            opacity: 0.9,
        },
        hint: {
            fontSize: "14px",
            color: "#9ca3af",
        },
        form: {
            marginTop: "4px",
        },
        formRow: {
            marginBottom: "12px",
        },
        label: {
            display: "block",
            fontSize: "14px",
            marginBottom: "4px",
            color: "#d1d5db",
        },
        selectedServiceName: {
            display: "block",
            marginTop: "4px",
            fontWeight: 600,
            color: "#f9fafb",
        },
        fileInput: {
            display: "block",
            marginTop: "6px",
            fontSize: "13px",
        },
        submitButton: {
            marginTop: "4px",
            padding: "8px 12px",
            fontSize: "14px",
            fontWeight: 600,
            borderRadius: "999px",
            border: "none",
            background: "linear-gradient(135deg, #22c55e 0%, #16a34a 50%, #22c55e 100%)",
            color: "#f9fafb",
            cursor: "pointer",
            boxShadow: "0 8px 20px rgba(34, 197, 94, 0.35)",
        },
        error: {
            color: "#fecaca",
            fontSize: "13px",
        },
        downloadLink: {
            display: "inline-block",
            marginTop: "4px",
            padding: "8px 12px",
            fontSize: "14px",
            borderRadius: "999px",
            border: "1px solid #38bdf8",
            color: "#e0f2fe",
            textDecoration: "none",
        },
        footer: {
            marginTop: "auto",
            textAlign: "center",
            paddingTop: "12px",
            fontSize: "12px",
            color: "#6b7280",
        },
        footerText: {
            margin: 0,
        },
    };

    export default App;

