/* global window, document, location, PlatformApi */
(function(){
  window.__PLATFORM_BRIDGE_VERSION__ = "20251228_03";

  const GAME_ID = (function(){
    // Preferred: game HTML declares window.__GAME_ID__ = "..."
    if (typeof window.__GAME_ID__ === "string" && window.__GAME_ID__) return window.__GAME_ID__;
    // Fallback: infer from URL like /games/<id>.html
    const m = location.pathname.match(/\/games\/([^\/]+)\.html$/);
    return m ? decodeURIComponent(m[1]) : "unknown";
  })();

  // Optional override: /games/parabola.html?menu=/some/path/
  const MENU_URL = (new URL(location.href)).searchParams.get("menu") || "/";

  function menuHrefWithNext(nextPath){
    try{
      const u = new URL(MENU_URL, location.origin);
      u.searchParams.set("next", nextPath);
      return u.pathname + u.search;
    } catch(_){
      return `/?next=${encodeURIComponent(nextPath)}`;
    }
  }


  const MODE = (new URL(location.href)).searchParams.get("mode") || "new";

  // -------- telemetry buffer (minimal, without heavy game instrumentation) --------
  const Telemetry = [];
  function nowMs(){ return Date.now(); }
  function push(type, data){
    Telemetry.push({ t: nowMs(), type, ...(data||{}) });
  }

  // -------- overlay UI --------
  function createOverlay(){
    const root = document.createElement("div");
    root.style.position = "fixed";
    root.style.top = "12px";
    root.style.right = "12px";
    root.style.zIndex = "99999";
    root.style.display = "flex";
    root.style.gap = "8px";
    root.style.alignItems = "center";
    root.style.padding = "8px";
    root.style.borderRadius = "14px";
    root.style.background = "rgba(10, 14, 26, 0.65)";
    root.style.border = "1px solid rgba(255,255,255,0.18)";
    root.style.backdropFilter = "blur(10px)";
    root.style.boxShadow = "0 12px 50px rgba(0,0,0,0.35)";

    const pill = document.createElement("div");
    pill.textContent = GAME_ID;
    pill.style.font = "600 12px system-ui, sans-serif";
    pill.style.color = "rgba(255,255,255,.85)";
    pill.style.padding = "6px 10px";
    pill.style.borderRadius = "999px";
    pill.style.background = "rgba(255,255,255,.06)";
    pill.style.border = "1px solid rgba(255,255,255,0.12)";

    const btnSave = document.createElement("button");
    btnSave.textContent = "Сохранить";
    styleBtn(btnSave, false);

    const btnExit = document.createElement("button");
    btnExit.textContent = "Выйти";
    styleBtn(btnExit, true);

    root.appendChild(pill);
    root.appendChild(btnSave);
    root.appendChild(btnExit);

    document.body.appendChild(root);
    return { root, btnSave, btnExit };
  }

  function styleBtn(btn, secondary){
    btn.style.font = "600 13px system-ui, sans-serif";
    btn.style.padding = "8px 10px";
    btn.style.borderRadius = "12px";
    btn.style.cursor = "pointer";
    btn.style.border = "1px solid rgba(255,255,255,0.16)";
    btn.style.color = "rgba(255,255,255,.92)";
    btn.style.background = secondary ? "rgba(255,255,255,0.06)" : "linear-gradient(180deg, rgba(93,214,255,.22), rgba(93,214,255,.06))";
  }

  function confirmDialog(message, opts){
    // opts: { okText, cancelText }
    return new Promise((resolve)=>{
      const back = document.createElement("div");
      back.style.position = "fixed";
      back.style.inset = "0";
      back.style.background = "rgba(0,0,0,.45)";
      back.style.zIndex = "100000";
      back.style.display = "grid";
      back.style.placeItems = "center";
      back.style.padding = "18px";

      const card = document.createElement("div");
      card.style.width = "min(520px, 100%)";
      card.style.background = "rgba(17,26,46,.92)";
      card.style.border = "1px solid rgba(255,255,255,0.16)";
      card.style.borderRadius = "16px";
      card.style.boxShadow = "0 18px 70px rgba(0,0,0,.45)";
      card.style.padding = "14px";

      const txt = document.createElement("div");
      txt.textContent = message;
      txt.style.color = "rgba(255,255,255,.92)";
      txt.style.font = "600 14px system-ui, sans-serif";
      txt.style.marginBottom = "12px";
      txt.style.lineHeight = "1.35";

      const row = document.createElement("div");
      row.style.display = "flex";
      row.style.gap = "10px";
      row.style.justifyContent = "flex-end";

      const cancel = document.createElement("button");
      cancel.textContent = (opts && opts.cancelText) || "Отмена";
      styleBtn(cancel, true);

      const ok = document.createElement("button");
      ok.textContent = (opts && opts.okText) || "OK";
      styleBtn(ok, false);

      cancel.onclick = ()=>{
        back.remove();
        resolve(false);
      };
      ok.onclick = ()=>{
        back.remove();
        resolve(true);
      };

      row.appendChild(cancel);
      row.appendChild(ok);
      card.appendChild(txt);
      card.appendChild(row);
      back.appendChild(card);
      document.body.appendChild(back);
    });
  }

  function toast(msg){
    const t = document.createElement("div");
    t.textContent = msg;
    t.style.position = "fixed";
    t.style.left = "50%";
    t.style.bottom = "18px";
    t.style.transform = "translateX(-50%)";
    t.style.zIndex = "100001";
    t.style.padding = "10px 14px";
    t.style.borderRadius = "14px";
    t.style.background = "rgba(10, 14, 26, 0.75)";
    t.style.border = "1px solid rgba(255,255,255,0.18)";
    t.style.color = "rgba(255,255,255,.92)";
    t.style.font = "600 13px system-ui, sans-serif";
    t.style.backdropFilter = "blur(10px)";
    document.body.appendChild(t);
    setTimeout(()=>t.remove(), 1400);
  }

  // -------- Game adapter (per-game functions are minimal and already exist in your HTML) --------
  function createGameInstance(){
    // Supported conventions:
    // 1) window.GameInstance.{exportState, importState, getSessionSummary, ...}  (per docs)
    // 2) Legacy globals: window.exportState/window.importState OR window.getState()
    const w = window;
    const inst = (w.GameInstance && typeof w.GameInstance === "object") ? w.GameInstance : {};

    const exportFn =
      (typeof w.getState === "function" ? w.getState
      : (typeof w.exportState === "function" ? w.exportState
      : (typeof inst.getState === "function" ? inst.getState.bind(inst)
      : (typeof inst.exportState === "function" ? inst.exportState.bind(inst)
      : null))));

    const importFn =
      (typeof w.importState === "function" ? w.importState
      : (typeof inst.importState === "function" ? inst.importState.bind(inst)
      : null));

    const resetFn =
      (typeof w.reset_to_new_game === "function" ? w.reset_to_new_game
      : (typeof w.resetToNewGame === "function" ? w.resetToNewGame
      : (typeof inst.reset_to_new_game === "function" ? inst.reset_to_new_game.bind(inst)
      : (typeof inst.resetToNewGame === "function" ? inst.resetToNewGame.bind(inst)
      : (typeof inst.resetToNew === "function" ? inst.resetToNew.bind(inst)
      : null)))));

    const summaryFn =
      (typeof w.getSessionSummary === "function" ? w.getSessionSummary
      : (typeof inst.getSessionSummary === "function" ? inst.getSessionSummary.bind(inst)
      : ()=>({})));

    const completedFn =
      (typeof w.isCompleted === "function" ? w.isCompleted
      : (typeof inst.isCompleted === "function" ? inst.isCompleted.bind(inst)
      : (typeof inst.isFinished === "function" ? inst.isFinished.bind(inst)
      : null)));

    // Fallback: if some functions are missing, we still support "exit without save"
    return {
      getState: exportFn ? ()=>exportFn() : ()=>null,
      importState: importFn ? (s)=>importFn(s) : null,
      resetToNew: resetFn ? ()=>resetFn() : null,
      getSessionSummary: ()=>{
        try { return summaryFn() || {}; } catch(e){ console.error(e); return {}; }
      },
      isCompleted: completedFn ? ()=>{
        try { return !!completedFn(); } catch(e){ console.error(e); return false; }
      } : null,
    };
  }

  let sessionId = null;
  let finishing = false;
  let overlay = null;
  let game = null;

  async function ensureAuthed(){
    const token = PlatformApi.getToken();
    const nextPath = location.pathname + location.search;

    if(!token){
      location.href = menuHrefWithNext(nextPath);
      return false;
    }

    // quick check
    const me = await PlatformApi.meGet();
    if(!me || !me.ok){
      PlatformApi.clearToken();
      location.href = menuHrefWithNext(nextPath);
      return false;
    }

    return true;
  }

  async function loadIfRequested(){
    if(MODE !== "load") return;
    if(!game.importState) { toast("Загрузка не поддерживается в этой игре."); return; }
    const res = await PlatformApi.saveGet(GAME_ID);
    if(!res.ok || !res.save || !res.save.payload){
      toast("Сохранение не найдено.");
      return;
    }
    try{
      game.importState(res.save.payload);
      toast("Сохранение загружено.");
      push("save_loaded", {});
    }catch(e){
      console.error(e);
      toast("Ошибка загрузки сохранения.");
    }
  }

  async function saveNow(){
    let payload = null;

    try{
      payload = (game && game.getState) ? game.getState() : null;
    } catch(e){
      console.error(e);
      toast("Ошибка экспорта состояния.");
      return { ok: false, error: "export_failed" };
    }

    if(payload === null || payload === undefined){
      toast("Нечего сохранять (игра не экспортирует состояние).");
      return { ok: false, error: "no_state" };
    }

    try{
      const res = await PlatformApi.savePut(GAME_ID, payload);
      if(res && res.ok){
        toast("Сохранено.");
        push("save_written", {});
        return { ok: true };
      }
      toast("Ошибка сохранения.");
      return { ok: false, error: (res && res.error) || "save_failed" };
    } catch(e){
      console.error(e);
      toast("Ошибка сохранения.");
      return { ok: false, error: "network" };
    }
  }

  async function finish(reason){
    if(finishing) return { ok: false, error: "already_finishing" };
    finishing = true;
    try{
      push("session_end_intent", { reason });

      const summary = (game && game.getSessionSummary) ? game.getSessionSummary() : {};
      const events = Telemetry.slice(0, 4000); // защитный лимит

      if(sessionId){
        try{
          await PlatformApi.sessionFinish(GAME_ID, sessionId, reason, summary, events);
        } catch(e){
          console.error(e);
        }
      }
      return { ok: true };
    } finally {
      finishing = false;
    }
  }

  async function exitFlow(){
    const wantSave = await confirmDialog("Выйти из игры? Сохранить перед выходом?", {
      okText: "Сохранить и выйти",
      cancelText: "Выйти без сохранения"
    });

    try{
      if(wantSave){
        await saveNow();
      }
      const reason = (game && game.isCompleted && game.isCompleted()) ? "finish" : "exit";
      await finish(reason);
    } catch(e){
      console.error(e);
    }

    location.href = MENU_URL;
  }

  async function boot(){
    const ok = await ensureAuthed();
    if(!ok) return;

    overlay = createOverlay();
    overlay.btnSave.onclick = saveNow;
    overlay.btnExit.onclick = exitFlow;

    game = createGameInstance();

    push("session_start", {
      game_id: GAME_ID,
      mode: MODE,
      ua: navigator.userAgent,
      w: window.innerWidth,
      h: window.innerHeight
    });

    const st = await PlatformApi.sessionStart(GAME_ID, { mode: MODE });
    if(st && st.ok) sessionId = st.session_id;

    // Start mode logic
    if(MODE === "new" && game.resetToNew){
      try{ game.resetToNew(); }catch(_){}
    }
    await loadIfRequested();

    // Safety: if user closes tab — try best-effort finish (no blocking)
    window.addEventListener("beforeunload", ()=>{
      // cannot await reliably; just fire and forget
      if(sessionId && !finishing){
        const summary = (game && game.getSessionSummary) ? game.getSessionSummary() : {};
        navigator.sendBeacon?.(
          `/api/games/${encodeURIComponent(GAME_ID)}/session/${encodeURIComponent(sessionId)}/finish`,
          new Blob([JSON.stringify({ reason:"unload", summary, events: Telemetry.slice(0, 2000) })], { type: "application/json" })
        );
      }
    });

    // Optional API for games (если позже захотите вызывать из кода игры)
    window.Platform = {
      save: saveNow,
      exit: exitFlow,
      finish: async (reason)=>{ await finish(reason || "finish"); },

      // For games: finish session (+ optional save) and return to main menu
      finishAndExit: async (reason)=>{
        try{ await saveNow(); } catch(e){ console.error(e); }
        try{ await finish(reason || "finish"); } catch(e){ console.error(e); }
        location.href = MENU_URL;
      },

      // Just return to menu (no save)
      toMenu: async (reason)=>{
        try{ await finish(reason || "exit"); } catch(e){ console.error(e); }
        location.href = MENU_URL;
      },

      track: (type, data)=>push(type, data),
    };
  }

  // Run after DOM is ready
  if(document.readyState === "loading"){
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();