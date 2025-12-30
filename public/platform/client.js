/* global window, fetch */
(function(){
  window.__PLATFORM_CLIENT_VERSION__ = "20251228_03";

  const TOKEN_KEY = "platform_token_v1";

  // Храним base, чтобы iframe-игры могли работать без прямого доступа к parent
  const API_BASE_KEY = "platform_api_base_v1";

  function normBase(v){
    return (typeof v === "string" ? v.trim() : "").replace(/\/+$/,"");
  }
  function readBaseFrom(w){
    if(!w) return "";
    // поддержим оба имени на всякий случай
    return normBase(w.__API_BASE__ || w.__API_BASE_URL__ || "");
  }
  function getApiBase(){
    const fromSelf = readBaseFrom(window);
    const fromParent = (window.parent && window.parent !== window) ? readBaseFrom(window.parent) : "";
    const fromStorage = normBase(localStorage.getItem(API_BASE_KEY) || "");

    const base = fromSelf || fromParent || fromStorage;
    if(base) localStorage.setItem(API_BASE_KEY, base);
    return base;
  }

  function getToken(){ return localStorage.getItem(TOKEN_KEY) || ""; }
  function setToken(t){ localStorage.setItem(TOKEN_KEY, t); }
  function clearToken(){ localStorage.removeItem(TOKEN_KEY); }

  async function request(method, path, body){
    const headers = { "Content-Type": "application/json" };
    const token = getToken();
    if(token) headers.Authorization = `Bearer ${token}`;

    const base = getApiBase();
    const url = base ? new URL(path, base).toString() : path;
    const res = await fetch(url, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });

    let data = null;
    try { data = await res.json(); } catch(_) {}
    if(!data){
      data = { ok: false, error: `HTTP ${res.status}` };
    }
    return data;
  }

  const PlatformApi = {
    // token
    getToken, setToken, clearToken,

    // auth
    authStart(phone){
      return request("POST", "/api/auth/start", { phone });
    },
    authVerify(phone, code){
      return request("POST", "/api/auth/verify", { phone, code });
    },

    // profile
    meGet(){
      return request("GET", "/api/me");
    },
    meSetName(name){
      return request("POST", "/api/me", { name });
    },

    // saves
    saveGet(gameId){
      return request("GET", `/api/games/${encodeURIComponent(gameId)}/save`);
    },
    savePut(gameId, save){
      return request("PUT", `/api/games/${encodeURIComponent(gameId)}/save`, { save });
    },
    saveDelete(gameId){
      return request("DELETE", `/api/games/${encodeURIComponent(gameId)}/save`);
    },

    // sessions
    sessionStart(gameId, meta){
      return request("POST", `/api/games/${encodeURIComponent(gameId)}/session/start`, { meta: meta || {} });
    },
    sessionFinish(gameId, sessionId, reason, summary, events){
      return request("POST", `/api/games/${encodeURIComponent(gameId)}/session/${encodeURIComponent(sessionId)}/finish`, {
        reason: reason || "exit",
        summary: summary || {},
        events: Array.isArray(events) ? events : []
      });
    },
  };

  window.PlatformApi = PlatformApi;
})();