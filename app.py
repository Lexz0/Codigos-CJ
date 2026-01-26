# -*- coding: utf-8 -*-
import os, io, time, json, base64, hashlib, re, random, unicodedata
from urllib.parse import quote, quote as urlquote
import requests
import pandas as pd
from flask import Flask, request, jsonify, render_template, send_file
from PIL import Image, ImageDraw, ImageFont
import msal
from pathlib import Path

# =====================================================================
# FLASK
# =====================================================================
app = Flask(__name__, template_folder="templates1")

@app.after_request
def no_cache_all(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

BASE_DIR = Path(__file__).resolve().parent

# =====================================================================
# REDIS
# =====================================================================
def _redis_base():
    url = os.environ["REDIS_URL"].rstrip("/")
    token = os.environ["REDIS_TOKEN"]
    headers = {"Authorization": f"Bearer {token}"}
    return url, headers

def redis_set(key, value, ttl_sec=None):
    url, headers = _redis_base()
    qs = f"?EX={int(ttl_sec)}" if ttl_sec else ""
    data = json.dumps(value)
    r = requests.post(f"{url}/set/{urlquote(key)}{qs}", headers=headers, data=data, timeout=15)
    r.raise_for_status()

def redis_get(key):
    url, headers = _redis_base()
    r = requests.get(f"{url}/get/{urlquote(key)}", headers=headers, timeout=15)
    r.raise_for_status()
    result = r.json().get("result")
    if result in (None, "(nil)"):
        return None
    try:
        return json.loads(result)
    except:
        return result

def redis_del(key):
    url, headers = _redis_base()
    r = requests.post(f"{url}/del/{urlquote(key)}", headers=headers, timeout=15)
    r.raise_for_status()

# =====================================================================
# CONFIG
# =====================================================================
BOT_TOKEN = os.environ["BOT_TOKEN"]
GROUP_CHAT_ID = int(os.environ["GROUP_CHAT_ID"])
BARKODER_SECRET = os.environ["BARKODER_SECRET"]
ONEDRIVE_SHARE_LINK = os.environ["ONEDRIVE_SHARE_LINK"]

EVIDENCIAS_FOLDER = "Evidencias CJ"  # carpeta en OneDrive para PNGs opcionales
ALUMNOS_XLSX_LOCAL = "alumnos.xlsx"
SHEET_ALUMNOS = "Control Asistencia"
SHEET_REGISTROS = "Registros"

FONT_TEXT = os.environ.get("FONT_TEXT", "NotoSans-Regular.ttf")
FONT_CODE39 = os.environ.get("FONT_CODE39", "IDAutomationHC39M.ttf")

CARD_W, CARD_H = 900, 500
CODE_REGEX = re.compile(r"^[0-9]{1,64}$")

AZURE_CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
AZURE_TENANT = os.environ.get("AZURE_TENANT", "consumers")
MSAL_CACHE_PATH = os.environ.get("MSAL_CACHE_PATH", "msal_cache.json")
SCOPES = ["User.Read", "Files.ReadWrite"]
AUTHORITY = f"https://login.microsoftonline.com/{AZURE_TENANT}"
GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
REDIS_LAST_ETAG_KEY = "alumnos:last_etag"

# =====================================================================
# MSAL TOKEN
# =====================================================================
def _load_cache():
    cache = msal.SerializableTokenCache()
    if os.path.exists(MSAL_CACHE_PATH):
        cache.deserialize(Path(MSAL_CACHE_PATH).read_text())
    return cache

def _save_cache(cache):
    if cache.has_state_changed:
        Path(MSAL_CACHE_PATH).write_text(cache.serialize())

def _acquire_token_silent():
    cache = _load_cache()
    client = msal.PublicClientApplication(
        AZURE_CLIENT_ID, authority=AUTHORITY, token_cache=cache
    )
    acc = client.get_accounts()
    if acc:
        tok = client.acquire_token_silent(SCOPES, account=acc[0])
        if tok and "access_token" in tok:
            return tok
    return None

def _get_token_or_raise():
    tok = _acquire_token_silent()
    if not tok:
        raise RuntimeError("No hay token. Ejecuta /init-auth.")
    return tok["access_token"]

# =====================================================================
# RETRY (genérico, por si lo necesitas)
# =====================================================================
def _retry(fn, desc, attempts=5, base_delay=1.5):
    for i in range(1, attempts+1):
        try:
            return fn()
        except requests.exceptions.HTTPError as e:
            sc = getattr(e.response, "status_code", None)
            if sc in (423, 429, 409) or (sc and 500 <= sc < 600):
                time.sleep(base_delay*(2**(i-1)) + random.random()*0.2)
                continue
            raise
        except requests.exceptions.RequestException:
            time.sleep(base_delay*(2**(i-1)) + random.random()*0.2)
            continue
    raise RuntimeError(f"{desc} no se pudo completar.")

# =====================================================================
# ONEDRIVE / GRAPH
# =====================================================================
def _resolve_share(token):
    enc = "u!" + base64.urlsafe_b64encode(
        ONEDRIVE_SHARE_LINK.encode("utf-8")
    ).decode("utf-8").rstrip("=")
    url = f"{GRAPH_ROOT}/shares/{enc}/driveItem"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}", "Prefer": "redeemSharingLink"}, timeout=30)
    r.raise_for_status()
    return r.json()

def get_item_meta(token):
    item = _resolve_share(token)
    r = requests.get(f"{GRAPH_ROOT}/drive/items/{item['id']}", headers={"Authorization": f"Bearer {token}"}, timeout=30)
    r.raise_for_status()
    return r.json()

def download_excel():
    token = _get_token_or_raise()
    item = _resolve_share(token)
    url = f"{GRAPH_ROOT}/drive/items/{item['id']}/content"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    r.raise_for_status()
    with open(ALUMNOS_XLSX_LOCAL, "wb") as f:
        f.write(r.content)

def download_excel_wait_fresh():
    # Simplificado: descarga y actualiza el eTag en Redis (sin bucles de espera)
    try:
        meta = get_item_meta(_get_token_or_raise())
        etag_now = meta.get("eTag")
    except:
        etag_now = None
    download_excel()
    try:
        if etag_now:
            redis_set(REDIS_LAST_ETAG_KEY, etag_now, 3600)
    except:
        pass

def upload_excel():
    token = _get_token_or_raise()
    item = _resolve_share(token)
    url = f"{GRAPH_ROOT}/drive/items/{item['id']}/content"
    # CORREGIDO: eliminar If-Match:"*" para evitar 412 con enlaces compartidos
    with open(ALUMNOS_XLSX_LOCAL, "rb") as f:
        r = requests.put(url, headers={"Authorization": f"Bearer {token}"}, data=f, timeout=120)
    r.raise_for_status()

def upload_image_to_onedrive(filename, content):
    """Sube un PNG de evidencia a la carpeta EVIDENCIAS_FOLDER en OneDrive."""
    token = _get_token_or_raise()
    # Asegurar carpeta
    url_check = f"{GRAPH_ROOT}/me/drive/root:/{quote(EVIDENCIAS_FOLDER)}"
    r = requests.get(url_check, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    if r.status_code == 404:
        r2 = requests.post(
            f"{GRAPH_ROOT}/me/drive/root/children",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"name": EVIDENCIAS_FOLDER, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"},
            timeout=30
        )
        r2.raise_for_status()
    elif r.status_code != 200:
        r.raise_for_status()
    # Subir archivo
    url_put = f"{GRAPH_ROOT}/me/drive/root:/{quote(EVIDENCIAS_FOLDER)}/{quote(filename)}:/content"
    r3 = requests.put(url_put, headers={"Authorization": f"Bearer {token}"}, data=content, timeout=120)
    r3.raise_for_status()

# =====================================================================
# TELEGRAM
# =====================================================================
def tg_send_message(chat_id, text):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=20)
    except:
        pass

def tg_send_photo(chat_id, png, caption):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
        files = {"photo": ("t.png", png, "image/png")}
        requests.post(url, data={"chat_id": chat_id, "caption": caption}, files=files, timeout=20)
    except:
        pass

# =====================================================================
# EXCEL HELPERS
# =====================================================================
def _ensure_cols(df):
    df.rename(columns=lambda c: str(c).strip(), inplace=True)
    for c in ["Tarjeta generada", "Conf. Bot"]:
        if c not in df.columns:
            df[c] = ""
    df["Tarjeta generada"] = df["Tarjeta generada"].astype(str).fillna("")
    df["Conf. Bot"] = df["Conf. Bot"].astype(str).fillna("")
    if "Código" in df.columns:
        df["Código"] = df["Código"].astype(str).str.strip()
    if "Nombres" in df.columns:
        df["Nombres"] = df["Nombres"].astype(str).str.strip()
    if "Apellidos" in df.columns:
        df["Apellidos"] = df["Apellidos"].astype(str).str.strip()
    if "Clase a la que asiste" in df.columns:
        df["Clase a la que asiste"] = df["Clase a la que asiste"].astype(str).str.strip()
    return df

def load_df():
    download_excel_wait_fresh()
    df = pd.read_excel(ALUMNOS_XLSX_LOCAL, sheet_name=SHEET_ALUMNOS, engine="openpyxl")
    return _ensure_cols(df)

def save_df_and_registro(df, codigo, nombre, clase, fecha, conf):
    df = _ensure_cols(df)
    if conf:
        try:
            idx = df.index[df["Código"] == codigo]
            if len(idx) > 0:
                df.loc[idx, "Conf. Bot"] = "Admitido"
        except:
            pass
    try:
        reg = pd.read_excel(ALUMNOS_XLSX_LOCAL, sheet_name=SHEET_REGISTROS, engine="openpyxl")
    except:
        reg = pd.DataFrame(columns=["timestamp", "codigo", "nombre", "clase", "fecha"])
    reg = pd.concat([reg, pd.DataFrame([{
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "codigo": codigo,
        "nombre": nombre,
        "clase": clase,
        "fecha": fecha
    }])], ignore_index=True)

    with pd.ExcelWriter(ALUMNOS_XLSX_LOCAL, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name=SHEET_ALUMNOS, index=False)
        reg.to_excel(w, sheet_name=SHEET_REGISTROS, index=False)

    upload_excel()

# =====================================================================
# ASISTENCIA DINÁMICA (encabezados que sean fecha + 'x')
# =====================================================================
def asistencia_dinamica(df, row_idx):
    date_cols = []
    for j, col in enumerate(df.columns):
        s = str(col).strip()
        ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.isna(ts):
            ts = pd.to_datetime(s, errors="coerce", dayfirst=False)
        if not pd.isna(ts):
            date_cols.append((j, ts))
    if not date_cols:
        return "", False

    marcas = []
    for j, ts in date_cols:
        val = df.iat[row_idx, j]
        mark = str(val).strip().lower() if val is not None else ""
        if mark == "x":
            marcas.append((j, ts))
    if not marcas:
        return "", False

    j_latest, _ = max(marcas, key=lambda x: x[1])
    return str(df.columns[j_latest]), True

# =====================================================================
# CÓDIGO — extracción robusta + normalización + Base64 si aplica
# =====================================================================
def extraer_codigo(data):
    keys = ["value", "textualData", "text", "barcodeData", "data", "code", "content"]

    def from_dict(d):
        if not isinstance(d, dict): return ""
        for k in keys:
            v = d.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        res = d.get("result")
        if isinstance(res, dict):
            for k in keys:
                v = res.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
        return ""

    if isinstance(data, dict):
        c = from_dict(data)
        if c: return c
    if isinstance(data, list):
        for e in data:
            c = from_dict(e)
            if c: return c
    return ""

def decode_base64_if_needed(s):
    """Si s es Base64 válido y decodifica a dígitos, devolver decodificado; si no, dejar igual."""
    try:
        decoded = base64.b64decode(s).decode("utf-8")
        if decoded.isdigit():
            return decoded
        return s
    except:
        return s

def normalizar_codigo(c):
    d = "".join(ch for ch in c if ch.isdigit())
    return d if d else c.strip()

# =====================================================================
# PROCESAR CÓDIGO
# =====================================================================
def procesar_codigo(codigo):
    df = load_df()
    if "Código" not in df.columns:
        return False, "No hay columna Código"

    fila = df.index[df["Código"] == codigo]
    if len(fila) == 0:
        tg_send_message(GROUP_CHAT_ID, f"⛔️ Código no encontrado: {codigo}")
        return False, "Código no encontrado"

    idx = fila[0]
    row = df.loc[idx]
    nombre = f"{row.get('Nombres','')} {row.get('Apellidos','')}".strip()
    clase = str(row.get("Clase a la que asiste","")).strip()
    ultima, puede = asistencia_dinamica(df, idx)

    save_df_and_registro(df, codigo, nombre, clase, time.strftime("%Y-%m-%d"), puede)

    veredicto = "✅ Admitido" if puede else "⛔️ No tiene asistencias suficientes"
    ult = ultima if ultima else "—"
    msg = (
        f"📘 {nombre}\n"
        f"• Clase: {clase}\n"
        f"• Última asistencia: {ult}\n"
        f"• Veredicto: {veredicto}\n"
        f"• Fecha/Hora: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    tg_send_message(GROUP_CHAT_ID, msg)
    return puede, msg

# =====================================================================
# BARKODER-SCAN (UNIFICADO Y CORREGIDO)
# =====================================================================
@app.post("/barkoder-scan")
def barkoder_scan():
    try:
        # body flexible
        body = request.get_json(silent=True)
        if not body:
            body = request.form.to_dict() if request.form else {}
        if not body:
            raw = request.get_data(as_text=True) or ""
            try:
                body = json.loads(raw)
            except:
                body = {}

        sec_data = str(
            body.get("security_data")
            or body.get("securityData")
            or ""
        ).strip()
        sec_hash = str(
            body.get("security_hash")
            or body.get("securityHash")
            or ""
        ).strip()

        if not sec_data or not sec_hash:
            tg_send_message(GROUP_CHAT_ID, "⚠️ Barkoder: parámetros incompletos")
            return jsonify(status=False, message="Parámetros incompletos"), 200

        exp1 = hashlib.md5((sec_data + BARKODER_SECRET).encode("utf-8")).hexdigest()
        exp2 = hashlib.md5((sec_data + BARKODER_SECRET.strip()).encode("utf-8")).hexdigest()
        if sec_hash not in (exp1, exp2):
            tg_send_message(GROUP_CHAT_ID, "⚠️ Barkoder: hash inválido")
            return jsonify(status=False, message="Hash inválido"), 200

        data_field = (
            body.get("data")
            or body.get("payload")
            or body.get("value")
            or body.get("result")
            or body
        )
        try:
            if isinstance(data_field, (dict, list)):
                data_json = data_field
            elif isinstance(data_field, str):
                try:
                    data_json = json.loads(base64.b64decode(data_field).decode("utf-8"))
                except:
                    data_json = json.loads(data_field)
            else:
                data_json = {}
        except:
            tg_send_message(GROUP_CHAT_ID, "⚠️ Barkoder: data inválido")
            return jsonify(status=False, message="data inválido"), 200

        raw_code = extraer_codigo(data_json)
        if not raw_code:
            tg_send_message(GROUP_CHAT_ID, "⚠️ Barkoder: no se encontró código")
            return jsonify(status=False, message="No se encontró código"), 200

        # NUEVO: decodificar Base64 si aplica
        raw_code = decode_base64_if_needed(raw_code)
        codigo = normalizar_codigo(raw_code)

        ok, msg = procesar_codigo(codigo)
        return jsonify(status=bool(ok), message=msg), 200

    except Exception as e:
        tg_send_message(GROUP_CHAT_ID, f"⚠️ Error en barkoder-scan: {e}")
        return jsonify(status=False, message=str(e)), 200

# =====================================================================
# TARJETAS PNG (Code39)
# =====================================================================
STRICT_BARCODE_FONT = True

def _load_font_safe(path_or_name, size):
    candidates = []
    p = str(path_or_name or "").strip()
    if p:
        candidates.append(p)
    candidates.append(str(BASE_DIR / "assets" / "fonts" / "IDAutomationHC39M.ttf"))
    candidates.append(str(BASE_DIR / "assets" / "fonts" / "Free3of9.ttf"))
    candidates.append("IDAutomationHC39M.ttf")
    candidates.append("Free3of9.ttf")
    if p:
        candidates.append(str(BASE_DIR / "assets" / "fonts" / p))
    last_err = None
    for cand in candidates:
        try:
            font = ImageFont.truetype(cand, size)
            return font
        except Exception as e:
            last_err = e
            continue
    if STRICT_BARCODE_FONT:
        raise RuntimeError(f"No se pudo cargar ninguna fuente válida: {last_err}")
    else:
        return ImageFont.load_default()

def crear_tarjeta(nombre, codigo):
    img = Image.new("RGB", (CARD_W, CARD_H), "white")
    draw = ImageDraw.Draw(img)
    f_name = _load_font_safe(FONT_TEXT, 64)
    f_bar  = _load_font_safe(FONT_CODE39, 160)
    f_txt  = _load_font_safe(FONT_TEXT, 36)

    # Nombre
    bbox_name = draw.textbbox((0, 0), nombre, font=f_name)
    draw.text(
        ((CARD_W - (bbox_name[2] - bbox_name[0])) / 2, 40),
        nombre,
        fill="black",
        font=f_name
    )

    # Código Code39
    bar = f"*{codigo}*"
    bbox_bar = draw.textbbox((0, 0), bar, font=f_bar)
    y_bar = (CARD_H // 2) - 60
    draw.text(
        ((CARD_W - (bbox_bar[2] - bbox_bar[0])) / 2, y_bar),
        bar,
        fill="black",
        font=f_bar
    )

    # Texto del código
    bbox_code = draw.textbbox((0, 0), codigo, font=f_txt)
    draw.text(
        ((CARD_W - (bbox_code[2] - bbox_code[0])) / 2, y_bar + (bbox_bar[3] - bbox_bar[1]) + 10),
        codigo,
        fill="#333333",
        font=f_txt
    )

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

# =====================================================================
# GENERAR TARJETAS (Batch)
# =====================================================================
def generar_tarjetas_y_enviar():
    df = load_df()
    hechos = 0
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")

    for i, row in df.iterrows():
        codigo = str(row.get("Código", "")).strip()
        if not codigo or not CODE_REGEX.match(codigo):
            continue

        tg_val = row.get("Tarjeta generada", "")
        if tg_val and tg_val.strip():
            continue

        nombre = f"{row.get('Nombres','')} {row.get('Apellidos','')}".strip()
        clase = str(row.get("Clase a la que asiste","")).strip()

        png = crear_tarjeta(nombre, codigo)
        tg_send_photo(GROUP_CHAT_ID, png, f"{nombre}\nClase: {clase}")

        # Subir evidencia a OneDrive (opcional)
        try:
            upload_image_to_onedrive(f"{nombre.replace(' ','_')}_{codigo}.png", png.getvalue())
        except:
            pass

        df.at[i, "Tarjeta generada"] = now_str
        hechos += 1

    if hechos > 0:
        df = _ensure_cols(df)
        with pd.ExcelWriter(ALUMNOS_XLSX_LOCAL, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
            df.to_excel(w, sheet_name=SHEET_ALUMNOS, index=False)
        upload_excel()

    return hechos

# =====================================================================
# UI / PREVIEW / REFRESH / PURGE
# =====================================================================
@app.get("/generar-tarjetas-ui")
def generar_tarjetas_ui():
    return render_template("generar_tarjetas.html")

@app.get("/generar-tarjetas-preview")
def generar_tarjetas_preview():
    try:
        df = load_df()
        resultados = []
        for _, row in df.iterrows():
            codigo = str(row.get("Código","")).strip()
            nombre = f"{row.get('Nombres','')} {row.get('Apellidos','')}".strip()
            clase = str(row.get("Clase a la que asiste","")).strip()
            motivo = []
            valido = True

            if not codigo:
                valido = False; motivo.append("Sin código")
            elif not CODE_REGEX.match(codigo):
                valido = False; motivo.append("Código inválido")
            if str(row.get("Tarjeta generada","")).strip():
                valido = False; motivo.append("Ya tenía tarjeta")

            resultados.append({
                "nombre": nombre or "—",
                "clase": clase or "—",
                "codigo": codigo or "—",
                "seria_generado": bool(valido),
                "motivo_si_no": ", ".join(motivo) if motivo else "OK"
            })

        total = len(resultados)
        candidatos = sum(r["seria_generado"] for r in resultados)
        return jsonify({
            "total_filas": total,
            "candidatos_a_generar": candidatos,
            "preview": resultados
        })
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.post("/force-refresh")
def force_refresh():
    try:
        if os.path.exists(ALUMNOS_XLSX_LOCAL):
            os.remove(ALUMNOS_XLSX_LOCAL)
        download_excel_wait_fresh()
        df = load_df()

        def is_candidate(row):
            codigo = str(row.get("Código","")).strip()
            tg = str(row.get("Tarjeta generada","")).strip()
            return codigo and CODE_REGEX.match(codigo) and not tg

        candidatos = int(df.apply(is_candidate, axis=1).sum())
        return jsonify(ok=True, total_filas=len(df), candidatos=candidatos)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@app.post("/purge-cache")
def purge_cache():
    try:
        if os.path.exists(ALUMNOS_XLSX_LOCAL):
            os.remove(ALUMNOS_XLSX_LOCAL)
        redis_del(REDIS_LAST_ETAG_KEY)
        download_excel_wait_fresh()
        df = load_df()

        def is_candidate(row):
            codigo = str(row.get("Código","")).strip()
            tg = str(row.get("Tarjeta generada","")).strip()
            return codigo and CODE_REGEX.match(codigo) and not tg

        candidatos = int(df.apply(is_candidate, axis=1).sum())
        return jsonify(ok=True, total_filas=len(df), candidatos=candidatos)
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

# =====================================================================
# GENERAR TARJETAS (POST/GET)
# =====================================================================
@app.route("/generar-tarjetas", methods=["POST", "GET"])
def generar_tarjetas():
    if request.method == "GET":
        return jsonify(status=True, message="Usa POST para generar las tarjetas.")
    try:
        hechas = generar_tarjetas_y_enviar()
        return jsonify(status=True, generadas=hechas)
    except Exception as e:
        return jsonify(status=False, error=str(e)), 500

# =====================================================================
# BK-TEST
# =====================================================================
@app.post("/bk-test")
def bk_test():
    try:
        raw = request.get_data(as_text=True) or ""
        ua = request.headers.get("User-Agent","")
        tg_send_message(GROUP_CHAT_ID, f"/bk-test hit\nUA: {ua}\nBody: {raw[:200]}")
    except:
        pass
    return jsonify(status=True, message="ok"), 200

# =====================================================================
# AUTH (init-auth / finish-auth / status)
# =====================================================================
@app.get("/init-auth")
def init_auth():
    try:
        cache = _load_cache()
        client = msal.PublicClientApplication(
            AZURE_CLIENT_ID, authority=AUTHORITY, token_cache=cache
        )
        flow = client.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            return jsonify(error="No se pudo crear device flow"), 500
        Path("flow.json").write_text(json.dumps(flow))
        return {
            "verification_uri": flow["verification_uri"],
            "user_code": flow["user_code"],
            "message": "Visita la URL y coloca este código."
        }
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.get("/finish-auth")
def finish_auth():
    try:
        if not Path("flow.json").exists():
            return jsonify(error="No hay flow pendiente"), 400
        flow = json.loads(Path("flow.json").read_text())
        cache = _load_cache()
        client = msal.PublicClientApplication(
            AZURE_CLIENT_ID, authority=AUTHORITY, token_cache=cache
        )
        result = client.acquire_token_by_device_flow(flow)
        if "access_token" in result:
            _save_cache(cache)
            Path("flow.json").unlink()
            return jsonify(success=True, message="Autenticado correctamente.")
        return jsonify(success=False, details=result)
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.get("/auth-status")
def auth_status():
    tok = _acquire_token_silent()
    return jsonify({"authenticated": bool(tok)})

# =====================================================================
# DEBUG DESCARGA DE EXCEL
# =====================================================================
@app.get("/_debug/alumnos.xlsx")
def _debug_excel():
    try:
        download_excel_wait_fresh()
        fname = f"alumnos_{int(time.time()*1000)}.xlsx"
        return send_file(ALUMNOS_XLSX_LOCAL, as_attachment=True, download_name=fname)
    except Exception as e:
        return jsonify(error=str(e)), 500

# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT","8080")))
