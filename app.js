function getApiBase() {
  return localStorage.getItem("apiBase") || "";
}
function saveApiBase() {
  const v = document.getElementById("apiBase").value.trim();
  localStorage.setItem("apiBase", v);
  alert("Сохранено");
  refresh();
}
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("apiBase").value = getApiBase();
  setTab("properties");
});

let TAB = "properties";

function setTab(t) {
  TAB = t;
  refresh();
}

async function api(path, method = "GET", body = null) {
  const base = getApiBase();
  if (!base) throw new Error("Укажи API base URL");
  const res = await fetch(base + path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : null
  });
  const txt = await res.text();
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = txt; }
  if (!res.ok) throw new Error((data && data.error) ? data.error : ("HTTP " + res.status));
  return data;
}

function el(html) {
  const d = document.createElement("div");
  d.innerHTML = html.trim();
  return d.firstChild;
}

async function refresh() {
  const root = document.getElementById("root");
  root.innerHTML = "";
  try {
    if (TAB === "properties") await renderProperties(root);
    if (TAB === "tenants") await renderTenants(root);
    if (TAB === "leases") await renderLeases(root);
  } catch (e) {
    root.appendChild(el(`<div class="card"><b>Ошибка:</b> ${e.message}</div>`));
  }
}

// --- Properties ---
async function renderProperties(root) {
  const list = await api("/properties");
  const card = el(`<div class="card" style="flex:1">
    <h2>Объекты</h2>
    <table>
      <thead><tr><th>Адрес</th><th>Статус</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>`);
  const tbody = card.querySelector("tbody");
  list.forEach(p => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(p.address || "")}</td>
      <td>${escapeHtml(p.status || "")}</td>
    `;
    tbody.appendChild(tr);
  });

  const form = el(`<div class="card">
    <h3>Добавить объект</h3>
    <input id="p_address" placeholder="Адрес" />
    <select id="p_status">
      <option>AVAILABLE</option>
      <option>RENTED</option>
      <option>MAINTENANCE</option>
    </select>
    <input id="p_notes" placeholder="Заметки (опционально)" />
    <button onclick="createProperty()">Создать</button>
  </div>`);

  root.appendChild(card);
  root.appendChild(form);
}

async function createProperty() {
  const address = document.getElementById("p_address").value.trim();
  const status = document.getElementById("p_status").value.trim();
  const notes = document.getElementById("p_notes").value;
  await api("/properties", "POST", { address, status, notes });
  refresh();
}

// --- Tenants ---
async function renderTenants(root) {
  const list = await api("/tenants");
  const card = el(`<div class="card" style="flex:1">
    <h2>Жильцы</h2>
    <table>
      <thead><tr><th>ФИО</th><th>Телефон</th><th>Email</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>`);
  const tbody = card.querySelector("tbody");
  list.forEach(t => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(t.full_name || "")}</td>
      <td>${escapeHtml(t.phone || "")}</td>
      <td>${escapeHtml(t.email || "")}</td>
    `;
    tbody.appendChild(tr);
  });

  const form = el(`<div class="card">
    <h3>Добавить жильца</h3>
    <input id="t_name" placeholder="ФИО" />
    <input id="t_phone" placeholder="Телефон" />
    <input id="t_email" placeholder="Email" />
    <button onclick="createTenant()">Создать</button>
  </div>`);

  root.appendChild(card);
  root.appendChild(form);
}

async function createTenant() {
  const full_name = document.getElementById("t_name").value.trim();
  const phone = document.getElementById("t_phone").value.trim();
  const email = document.getElementById("t_email").value.trim();
  await api("/tenants", "POST", { full_name, phone, email });
  refresh();
}

// --- Leases ---
async function renderLeases(root) {
  const leases = await api("/leases");
  const props = await api("/properties");
  const tenants = await api("/tenants");

  const card = el(`<div class="card" style="flex:1">
    <h2>Заселения</h2>
    <table>
      <thead><tr><th>Объект</th><th>Жилец</th><th>Start</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>`);
  const tbody = card.querySelector("tbody");

  function pName(id){ return (props.find(x=>x.property_id===id)||{}).address || id; }
  function tName(id){ return (tenants.find(x=>x.tenant_id===id)||{}).full_name || id; }

  leases.forEach(l => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(pName(l.property_id))}</td>
      <td>${escapeHtml(tName(l.tenant_id))}</td>
      <td>${escapeHtml(String(l.start_date || ""))}</td>
    `;
    tbody.appendChild(tr);
  });

  const form = el(`<div class="card">
    <h3>Новое заселение</h3>
    <select id="l_prop"></select>
    <select id="l_tenant"></select>
    <input id="l_start" placeholder="start_date YYYY-MM-DD" />
    <button onclick="createLease()">Создать</button>
    <div class="muted">Для простоты end_date не используем.</div>
  </div>`);

  const selP = form.querySelector("#l_prop");
  props.forEach(p => {
    const o = document.createElement("option");
    o.value = p.property_id;
    o.textContent = `${p.address} (${p.status})`;
    selP.appendChild(o);
  });

  const selT = form.querySelector("#l_tenant");
  tenants.forEach(t => {
    const o = document.createElement("option");
    o.value = t.tenant_id;
    o.textContent = t.full_name;
    selT.appendChild(o);
  });

  root.appendChild(card);
  root.appendChild(form);
}

async function createLease() {
  const property_id = document.getElementById("l_prop").value;
  const tenant_id = document.getElementById("l_tenant").value;
  const start_date = document.getElementById("l_start").value.trim();
  await api("/leases", "POST", { property_id, tenant_id, start_date });
  refresh();
}

function escapeHtml(s) {
  return String(s).replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
}
