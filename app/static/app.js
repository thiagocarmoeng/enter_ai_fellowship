document.addEventListener("DOMContentLoaded", () => {
  console.log("[ENTER UI] ready");

  const $ = (s) => document.querySelector(s);

  // --- refs de UI ---
  const fileInput    = $("#fileInput");
  const browseBtn    = $("#browseBtn");   // pode ser <button> OU <label for="fileInput">
  const dropzone     = $("#dropzone");
  const sendBtn      = $("#sendBtn");
  const useLLM       = $("#useLLM");
  const resultGrid   = $("#resultGrid");
  const debugBox     = $("#debug");
  const fileInfo     = $("#fileInfo");
  const fileNameEl   = $("#fileName");
  const fileSizeEl   = $("#fileSize");
  const progress     = $("#progress");
  const progressBar  = $("#progressBar");
  const apiBase      = $("#apiBase");
  const healthBtn    = $("#healthBtn");

  const schemaInput  = $("#schemaInput"); // NEW (textarea) — obrigatório
  const presetAll    = $("#presetAll");   // NEW (botão opcional)
  const presetOAB    = $("#presetOAB");   // NEW (botão opcional)
  const presetTela   = $("#presetTela");  // NEW (botão opcional)

  const tabBtnResult = document.querySelector('.tab[data-tab="result"]');
  const tabBtnDebug  = document.querySelector('.tab[data-tab="debug"]');
  const panelResult  = $("#panel-result");
  const panelDebug   = $("#panel-debug");

  if (!fileInput || !dropzone) {
    console.error("[ENTER UI] elementos-chave não encontrados (#fileInput/#dropzone).");
    return;
  }

  let currentFile = null;

  // --- utils ---
  const fmtBytes = (n) => {
    if (!n) return "0 B";
    const u = ["B","KB","MB","GB","TB"];
    const i = Math.floor(Math.log(n)/Math.log(1024));
    return (n/Math.pow(1024,i)).toFixed(2) + " " + u[i];
  };

  const setProgress = (v) => { if (progressBar) progressBar.style.width = `${v}%`; };

  const setTabs = (which) => {
    [tabBtnResult, tabBtnDebug].filter(Boolean).forEach(b => b.classList.remove("active"));
    [panelResult, panelDebug].filter(Boolean).forEach(p => p.classList.remove("active"));
    if (which === "debug") {
      tabBtnDebug?.classList.add("active");
      panelDebug?.classList.add("active");
    } else {
      tabBtnResult?.classList.add("active");
      panelResult?.classList.add("active");
    }
  };

  tabBtnResult?.addEventListener("click", () => setTabs("result"));
  tabBtnDebug?.addEventListener("click", () => setTabs("debug"));

  const renderKV = (label, value) => {
    const item = document.createElement("div");
    item.className = "kv-item";
    const l = document.createElement("div");
    l.className = "kv-label"; l.textContent = label;
    const v = document.createElement("div");
    v.className = "kv-value";
    if (value === null || value === undefined || String(value).trim() === "") {
      v.classList.add("empty"); v.textContent = "— vazio —";
    } else {
      v.textContent = String(value);
    }
    item.appendChild(l); item.appendChild(v);
    return item;
  };

  const renderExtraction = (extraction) => {
    if (!resultGrid) return;
    resultGrid.innerHTML = "";
    resultGrid.classList.remove("empty");
    const keys = Object.keys(extraction || {});
    if (!keys.length) {
      resultGrid.classList.add("empty");
      resultGrid.innerHTML = `<div class="placeholder">Nenhum resultado.</div>`;
      return;
    }
    for (const k of keys) resultGrid.appendChild(renderKV(k, extraction[k]));
  };

  // --- presets de schema (opcionais) ---
  presetAll?.addEventListener("click", () => {
    if (schemaInput) schemaInput.value = "ALL";
  });

  presetOAB?.addEventListener("click", () => {
    if (!schemaInput) return;
    schemaInput.value = JSON.stringify({
      nome: "", inscricao: "", seccional: "", subsecao: "",
      categoria: "", endereco_profissional: "", telefone_profissional: "", situacao: ""
    }, null, 2);
  });

  presetTela?.addEventListener("click", () => {
    if (!schemaInput) return;
    schemaInput.value = JSON.stringify({
      data_referencia: "", selecao_de_parcelas: "", total_de_parcelas: "",
      pesquisa_por: "", pesquisa_tipo: "", sistema: "", valor_parcela: "", cidade: "",
      data_base: "", data_verncimento: "", quantidade_parcelas: "", produto: "",
      tipo_de_operacao: "", tipo_de_sistema: ""
    }, null, 2);
  });

  // --- seleção de arquivo (sem duplicar diálogos) ---
  const isLabelForFile = !!(browseBtn && browseBtn.tagName === "LABEL" && browseBtn.getAttribute("for") === "fileInput");
  if (browseBtn && !isLabelForFile) {
    browseBtn.addEventListener("click", (e) => {
      e.preventDefault();
      fileInput.click();
    });
  }

  dropzone.addEventListener("click", (e) => {
    if (e.target && e.target.closest('label[for="fileInput"]')) return;
    fileInput.click();
  });

  // Arrastar/soltar
  ["dragenter","dragover"].forEach(evt => {
    dropzone.addEventListener(evt, e => {
      e.preventDefault(); e.stopPropagation();
      dropzone.classList.add("dragover");
    });
  });
  ["dragleave","drop"].forEach(evt => {
    dropzone.addEventListener(evt, e => {
      e.preventDefault(); e.stopPropagation();
      dropzone.classList.remove("dragover");
    });
  });
  dropzone.addEventListener("drop", (e) => {
    const f = e.dataTransfer?.files?.[0];
    if (f && (f.type === "application/pdf" || f.name.toLowerCase().endsWith(".pdf"))) {
      currentFile = f;
      fileNameEl && (fileNameEl.textContent = f.name);
      fileSizeEl && (fileSizeEl.textContent = fmtBytes(f.size));
      fileInfo && (fileInfo.hidden = false);
      sendBtn && (sendBtn.disabled = false);
      setTabs("result");
    } else {
      alert("Por favor, solte um arquivo PDF válido.");
    }
  });

  // Mudança no input de arquivo
  fileInput.addEventListener("change", () => {
    const f = fileInput.files && fileInput.files[0];
    if (f) {
      currentFile = f;
      fileNameEl && (fileNameEl.textContent = f.name);
      fileSizeEl && (fileSizeEl.textContent = fmtBytes(f.size));
      fileInfo && (fileInfo.hidden = false);
      sendBtn && (sendBtn.disabled = false);
      setTabs("result");
    }
  });

  // --- envio para /extract ---
  async function postExtract(){
    if (!currentFile || !sendBtn) return;

    const endpoint = (apiBase && apiBase.value.trim()) || "/extract";

    // schema obrigatório: "ALL" ou JSON
    let schemaVal = (schemaInput?.value || "").trim();
    if (!schemaVal) {
      alert("Informe o extraction_schema (ALL ou JSON).");
      return;
    }
    if (schemaVal.toUpperCase() !== "ALL") {
      try {
        JSON.parse(schemaVal); // valida no cliente para UX melhor
      } catch {
        alert("extraction_schema inválido: use 'ALL' ou um JSON válido.");
        return;
      }
    }

    const form = new FormData();
    form.append("file", currentFile, currentFile.name);
    if (useLLM) form.append("use_llm", useLLM.checked ? "true" : "false");
    form.append("extraction_schema", schemaVal);

    // UI
    sendBtn.disabled = true;
    if (progress) progress.hidden = false;
    setProgress(15);
    if (resultGrid) {
      resultGrid.innerHTML = `<div class="placeholder">Enviando…</div>`;
      resultGrid.classList.remove("empty");
    }
    if (debugBox) {
      debugBox.textContent = "—";
      debugBox.classList.add("json-view");
    }

    try {
      const res  = await fetch(endpoint, { method: "POST", body: form });
      setProgress(70);
      let json;
      try {
        json = await res.json();
      } catch {
        throw new Error(`Resposta inválida do servidor (status ${res.status}).`);
      }
      setProgress(100);

      if (!res.ok || json?.ok === false) {
        const msg = json?.error || `Falha na extração (status ${res.status}).`;
        throw new Error(msg);
      }

      renderExtraction(json.extraction_schema || {});
      if (debugBox) {
        debugBox.textContent = json.debug
          ? JSON.stringify(json.debug, null, 2)
          : "Sem debug retornado pela API (API_DEBUG=0?).";
      }
      setTabs("result");
    } catch (err) {
      if (resultGrid) resultGrid.innerHTML = `<div class="placeholder">Erro: ${String(err.message || err)}</div>`;
      if (debugBox)  debugBox.textContent = "Verifique o console do servidor e o CORS.";
      setTabs("result");
    } finally {
      setTimeout(() => { if (progress) progress.hidden = true; setProgress(0); }, 400);
      sendBtn.disabled = false;
    }
  }

  sendBtn?.addEventListener("click", postExtract);

  // Health
  healthBtn?.addEventListener("click", async () => {
    const base = (apiBase && apiBase.value.trim()) || "/extract";
    const url = base.endsWith("/extract") ? base.replace(/\/extract$/, "/health") : (base + "/health");
    try {
      const res = await fetch(url);
      const json = await res.json();
      alert(`Health: ${json?.ok ? "OK" : "Falhou"}\nLLM disponível: ${json?.llm?.available}\nProvider: ${json?.llm?.provider}\nModel: ${json?.llm?.model}`);
    } catch (e) {
      alert("Falha ao consultar /health.");
    }
  });

  // Estado inicial amigável
  if (schemaInput && !schemaInput.value.trim()) {
    schemaInput.value = "ALL";
  }
});
