# -*- coding: utf-8 -*-
import os, io, time, json, base64, hashlib, re, random
from urllib.parse import quote, quote as urlquote
import requests
import pandas as pd
from flask import Flask, request, jsonify, render_template
from PIL import Image, ImageDraw, ImageFont
import msal
from pathlib import Path

# =====================================================================
# FLASK (usa tu carpeta de plantillas "templates1")
# =====================================================================
app = Flask(__name__, template_folder="templates1")

# =====================================================================
# Upstash Redis REST
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
    return True

def redis_get(key):
    url, headers = _redis_base()
    r = requests.get(f"{url}/get/{urlquote(key)}", headers=headers, timeout=15)
    r.raise_for_status()
    result = r.json().get("result")
    if result in (None, "(nil)"):
        return None
    try:
        return json.loads(result)
    except Exception:
        return result

def redis_del(key):
    url, headers = _redis_base()
    r = requests.post(f"{url}/del/{urlquote(key)}", headers=headers, timeout=15)
    r.raise_for_status()
    return True

# =====================================================================
# CONFIG GENERAL
# =====================================================================
BOT_TOKEN = os.environ["BOT_TOKEN"]
GROUP_CHAT_ID = int(os.environ["GROUP_CHAT_ID"])
BARKODER_SECRET = os.environ["BARKODER_SECRET"]
ONEDRIVE_SHARE_LINK = os.environ["ONEDRIVE_SHARE_LINK"]
EVIDENCIAS_FOLDER = "Evidencias CJ"

ALUMNOS_XLSX_LOCAL = "alumnos.xlsx"
SHEET_ALUMNOS = "Control Asistencia"
SHEET_REGISTROS = "Registros"

FONT_TEXT = os.environ.get("FONT_TEXT", "NotoSans-Regular.ttf")
FONT_CODE39 = os.environ.get("FONT_CODE39", "IDAutomationHC39M.ttf")

CARD_W, CARD_H = 900, 500
CODE_REGEX = re.compile(r"^[A-Za-z0-9\-\_]{3,64}$")

AZURE_CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
AZURE_TENANT = os.environ.get("AZURE_TENANT", "consumers")
MSAL_CACHE_PATH = os.environ.get("MSAL_CACHE_PATH", "msal_cache.json")

SCOPES = ["User.Read", "Files.ReadWrite"]
AUTHORITY = f"https://login.microsoftonline.com/{AZURE_TENANT}"
GRAPH_ROOT = "https://graph.microsoft.com/v1.0"

# =====================================================================
# MSAL (auth)
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
    accounts = client.get_accounts()
    if accounts:
        result = client.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            return result
    return None

def _get_token_or_raise():
    tok = _acquire_token_silent()
    if not tok:
        raise RuntimeError("No hay token de Graph. Ejecuta /init-auth y luego /finish-auth.")
    return tok["access_token"]

# =====================================================================
# Reintentos (anti-423/429/409 y 5xx)
# =====================================================================
def _retry(fn, desc, max_attempts=5, base_delay=1.5):
    """
    Reintenta fn() capturando:
      - 423 Locked, 429 Throttled, 409 Conflict
      - 5xx transitorios
      - errores de red (RequestException)
    Backoff exponencial con jitter.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status in (423, 429, 409) or (status and 500 <= status < 600):
                sleep = base_delay * (2 ** (attempt - 1)) + 0.2 * random.random()
                app.logger.warning(f"{desc} falló con {status}. Reintentando {attempt}/{max_attempts} en {sleep:.1f}s...")
                time.sleep(sleep)
                continue
            raise
        except requests.exceptions.RequestException as e:
            sleep = base_delay * (2 ** (attempt - 1)) + 0.2 * random.random()
            app.logger.warning(f"{desc} error de red {e}. Reintentando {attempt}/{max_attempts} en {sleep:.1f}s...")
            time.sleep(sleep)
            continue
    raise RuntimeError(f"{desc} no se pudo completar tras {max_attempts} intentos.")

# =====================================================================
# OneDrive / Graph
# =====================================================================
def _resolve_share(token):
    encoded = "u!" + base64.urlsafe_b64encode(
        ONEDRIVE_SHARE_LINK.encode("utf-8")
    ).decode("utf-8").rstrip("=")

    url = f"{GRAPH_ROOT}/shares/{encoded}/driveItem"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}", "Prefer": "redeemSharingLink"},
        timeout=30
    )
    r.raise_for_status()
    return r.json()

def get_item_meta(token):
    """Devuelve metadatos del item compartido (incluye eTag y name)."""
    item = _resolve_share(token)
    item_id = item["id"]
    url = f"{GRAPH_ROOT}/drive/items/{item_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    r.raise_for_status()
    return r.json()

def download_excel():
    token = _get_token_or_raise()
    item = _resolve_share(token)
    item_id = item["id"]
    url = f"{GRAPH_ROOT}/drive/items/{item_id}/content"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    r.raise_for_status()
    with open(ALUMNOS_XLSX_LOCAL, "wb") as f:
        f.write(r.content)

def upload_excel_via_session(token, item_id):
    """Sube ALUMNOS_XLSX_LOCAL usando upload session (chunk único para archivo pequeño)."""
    # 1) Crear sesión con replace
    sess_url = f"{GRAPH_ROOT}/drive/items/{item_id}/createUploadSession"
    sess_body = {
        "item": {
            "@microsoft.graph.conflictBehavior": "replace",
            "name": Path(ALUMNOS_XLSX_LOCAL).name
        }
    }
    r = requests.post(
        sess_url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=sess_body,
        timeout=30
    )
    r.raise_for_status()
    upload_url = r.json()["uploadUrl"]

    # 2) Subir el contenido en un solo bloque
    data = Path(ALUMNOS_XLSX_LOCAL).read_bytes()
    total = len(data)
    headers = {
        "Content-Length": str(total),
        "Content-Range": f"bytes 0-{total-1}/{total}"
    }
    r2 = requests.put(upload_url, headers=headers, data=data, timeout=180)
    r2.raise_for_status()
    return True

def upload_excel():
    """
    Estrategia robusta:
      A) PUT con If-Match: *
      B) Si 412 → obtener eTag y PUT con If-Match: "<etag>"
      C) Si vuelve a fallar → upload session (replace)
    """
    token = _get_token_or_raise()
    item = _resolve_share(token)
    item_id = item["id"]
    url = f"{GRAPH_ROOT}/drive/items/{item_id}/content"

    # --- A) If-Match: * ---
    try:
        with open(ALUMNOS_XLSX_LOCAL, "rb") as f:
            r = requests.put(
                url,
                headers={"Authorization": f"Bearer {token}", "If-Match": "*"},
                data=f,
                timeout=120
            )
        r.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        if getattr(e.response, "status_code", None) != 412:
            raise

    # --- B) Con eTag actual ---
    try:
        meta = get_item_meta(token)
        etag = meta.get("eTag") or meta.get("@microsoft.graph.etag")
        if not etag:
            raise RuntimeError("No se pudo obtener eTag del archivo.")

        with open(ALUMNOS_XLSX_LOCAL, "rb") as f:
            r = requests.put(
                url,
                headers={"Authorization": f"Bearer {token}", "If-Match": etag},
                data=f,
                timeout=120
            )
        r.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        if getattr(e.response, "status_code", None) != 412:
            raise

    # --- C) Upload session (replace) ---
    upload_excel_via_session(token, item_id)
    return True

def upload_image_to_onedrive(filename, content):
    token = _get_token_or_raise()

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

    url_put = f"{GRAPH_ROOT}/me/drive/root:/{quote(EVIDENCIAS_FOLDER)}/{quote(filename)}:/content"
    r3 = requests.put(url_put, headers={"Authorization": f"Bearer {token}"}, data=content, timeout=120)
    r3.raise_for_status()

# ----- Versiones "seguras" con reintentos -----
def safe_download_excel():
    return _retry(download_excel, "download_excel")

def safe_upload_excel():
    return _retry(upload_excel, "upload_excel")

def safe_upload_image_to_onedrive(filename, content):
    return _retry(lambda: upload_image_to_onedrive(filename, content), f"upload_image_to_onedrive({filename})")

# =====================================================================
# Telegram
# =====================================================================
def tg_send_photo(chat_id, png_bytesio, caption=""):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {"photo": ("tarjeta.png", png_bytesio, "image/png")}
    data = {"chat_id": chat_id, "caption": caption}
    r = requests.post(url, data=data, files=files, timeout=30)
    r.raise_for_status()
    return r.json()

def tg_send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=30)
    r.raise_for_status()
    return r.json()

# =====================================================================
# Excel (carga/guarda)
# =====================================================================
def load_df():
    safe_download_excel()
    df = pd.read_excel(ALUMNOS_XLSX_LOCAL, sheet_name=SHEET_ALUMNOS, engine="openpyxl")
    if "Tarjeta generada" not in df.columns:
        df["Tarjeta generada"] = ""
    return df

def save_df_and_append_registro(df, codigo, nombre, clase, fecha, conf=False):
    if conf:
        idx = df.index[df["Código"].astype(str).str.strip() == codigo]
        if len(idx) > 0:
            df.loc[idx, "Conf. Bot"] = "Admitido"

    try:
        xls = pd.ExcelFile(ALUMNOS_XLSX_LOCAL, engine="openpyxl")
        regs = pd.read_excel(xls, sheet_name=SHEET_REGISTROS, engine="openpyxl")
    except Exception:
        regs = pd.DataFrame(columns=["timestamp","codigo","nombre","clase","fecha"])

    regs = pd.concat([regs, pd.DataFrame([{
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "codigo": codigo,
        "nombre": nombre,
        "clase": clase,
        "fecha": fecha
    }])], ignore_index=True)

    with pd.ExcelWriter(ALUMNOS_XLSX_LOCAL, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name=SHEET_ALUMNOS, index=False)
        regs.to_excel(w, sheet_name=SHEET_REGISTROS, index=False)

    safe_upload_excel()

# =====================================================================
# PNG Tarjetas
# =====================================================================
def crear_tarjeta(nombre, codigo):
    img = Image.new("RGB", (CARD_W, CARD_H), "white")
    draw = ImageDraw.Draw(img)

    f_name = ImageFont.truetype(FONT_TEXT, 64)
    f_bar  = ImageFont.truetype(FONT_CODE39, 160)
    f_txt  = ImageFont.truetype(FONT_TEXT, 36)

    tw, _ = draw.textsize(nombre, font=f_name)
    draw.text(((CARD_W - tw)/2, 40), nombre, fill="black", font=f_name)

    bar = f"*{codigo}*"
    bw, bh = draw.textsize(bar, font=f_bar)
    y_bar = (CARD_H // 2) - 60
    draw.text(((CARD_W - bw)/2, y_bar), bar, fill="black", font=f_bar)

    tw2, _ = draw.textsize(codigo, font=f_txt)
    draw.text(((CARD_W - tw2)/2, y_bar + bh + 10), codigo, fill="#333333", font=f_txt)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

# =====================================================================
# Generar tarjetas (batch)
# =====================================================================
def generar_tarjetas_y_enviar():
    df = load_df()
    hechos = 0

    for i, row in df.iterrows():
        codigo = str(row.get("Código","")).strip()
        if not codigo or not CODE_REGEX.match(codigo):
            continue

        if str(row.get("Tarjeta generada","")).strip():
            continue

        nombre = f"{row.get('Nombres','')} {row.get('Apellidos','')}".strip()
        clase = str(row.get("Clase a la que asiste","")).strip()

        png = crear_tarjeta(nombre, codigo)
        tg_send_photo(GROUP_CHAT_ID, png, f"{nombre}\nClase: {clase}")

        safe_upload_image_to_onedrive(f"{nombre.replace(' ','_')}_{codigo}.png", png.getvalue())

        df.at[i, "Tarjeta generada"] = time.strftime("%Y-%m-%d %H:%M:%S")
        hechos += 1

    with pd.ExcelWriter(ALUMNOS_XLSX_LOCAL, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name=SHEET_ALUMNOS, index=False)

    safe_upload_excel()
    return hechos


@app.get("/generar-tarjetas-preview")
def generar_tarjetas_preview():
    try:
        df = load_df()
        resultados = []
        total = len(df)

        for _, row in df.iterrows():
            codigo_raw = str(row.get("Código", "")).strip()
            nombre = f"{str(row.get('Nombres','')).strip()} {str(row.get('Apellidos','')).strip()}".strip()
            clase  = str(row.get("Clase a la que asiste","")).strip()
            tg     = str(row.get("Tarjeta generada","")).strip()

            motivo = []
            valido = True

            if not codigo_raw:
                valido = False
                motivo.append("Sin código")
            elif not CODE_REGEX.match(codigo_raw):
                valido = False
                motivo.append("Código no cumple regex (solo A-Z, a-z, 0-9, '-', '_')")

            if tg:
                valido = False
                motivo.append("Ya tenía 'Tarjeta generada'")

            resultados.append({
                "nombre": nombre,
                "clase": clase,
                "codigo": codigo_raw,
                "seria_generado": bool(valido),
                "motivo_si_no": ", ".join(motivo) if motivo else "OK"
            })

        candidatos = sum(1 for r in resultados if r["seria_generado"])
        return jsonify({
            "total_filas": total,
            "candidatos_a_generar": candidatos,
            "preview": resultados
        })
    except Exception as e:
        app.logger.exception("preview error")
        return jsonify(error=str(e)), 500


# =====================================================================
# Webhook Barkoder
# =====================================================================
@app.post("/barkoder-scan")
def barkoder_scan():
    try:
        body = request.get_json(force=True, silent=True) or {}
        security_data = str(body.get("security_data","")).strip()
        security_hash = str(body.get("security_hash","")).strip()
        data_field = body.get("data")

        if not security_data or not security_hash or data_field is None:
            return jsonify(status=False, message="Parámetros incompletos"), 200

        expected = hashlib.md5((security_data + BARKODER_SECRET).encode("utf-8")).hexdigest()
        if security_hash != expected:
            return jsonify(status=False, message="Hash inválido"), 200

        try:
            if isinstance(data_field, str):
                try:
                    decoded = base64.b64decode(data_field)
                    data_json = json.loads(decoded.decode("utf-8"))
                except Exception:
                    data_json = json.loads(data_field)
            else:
                data_json = data_field
        except Exception as e:
            return jsonify(status=False, message=f"data inválido: {e}"), 200

        codigo = None
        if isinstance(data_json, list):
            if len(data_json) == 0:
                return jsonify(status=False, message="Lista vacía"), 200
            elem = data_json[0]
            if isinstance(elem, dict):
                codigo = elem.get("value","") or elem.get("codevalue","")
        elif isinstance(data_json, dict):
            codigo = data_json.get("value","") or data_json.get("codevalue","")

        if not codigo:
            return jsonify(status=False, message="No se encontró 'value'"), 200

        codigo = str(codigo).strip()
        ok, msg = procesar_codigo(codigo)
        return jsonify(status=bool(ok), message=msg), 200

    except Exception as e:
        app.logger.exception("Error en barkoder-scan")
        return jsonify(status=False, message=f"Excepción: {e}"), 200

# =====================================================================
# Procesar códigos
# =====================================================================
def procesar_codigo(codigo):
    if not CODE_REGEX.match(codigo):
        return False, "Código inválido"

    df = load_df()
    fila = df.loc[df["Código"].astype(str).str.strip() == codigo]

    if fila.empty:
        return False, "Código no encontrado"

    nombre = f"{fila.iloc[0].get('Nombres','')} {fila.iloc[0].get('Apellidos','')}".strip()
    clase = str(fila.iloc[0].get("Clase a la que asiste","")).strip()
    juega = str(fila.iloc[0].get("Juega?","")).strip().lower()
    fecha = time.strftime("%Y-%m-%d")

    if juega in ("si","sí"):
        save_df_and_append_registro(df, codigo, nombre, clase, fecha, conf=True)
        tg_send_message(GROUP_CHAT_ID, f"✅ {nombre} — {clase} — {fecha}\nPuede ingresar.")
        return True, f"Registrado: {nombre}"
    else:
        save_df_and_append_registro(df, codigo, nombre, clase, fecha, conf=False)
        tg_send_message(GROUP_CHAT_ID, f"⚠️ {nombre} — {clase} — {fecha}\nNO tiene asistencias registradas.")
        return False, "Sin asistencia"

# =====================================================================
# Auth MS Graph
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

        ttl = int(flow.get("expires_in", 900))
        redis_set("device_flow", flow, ttl_sec=ttl)

        return jsonify({
            "verification_uri": flow["verification_uri"],
            "user_code": flow["user_code"],
            "message": "Visita la URL y coloca el código para autorizar."
        })
    except Exception as e:
        app.logger.exception("init-auth error")
        return jsonify(error=str(e)), 500

@app.get("/finish-auth")
def finish_auth():
    try:
        flow = redis_get("device_flow")
        if not flow:
            return jsonify(error="No hay flow pendiente. Ejecuta /init-auth primero."), 400

        cache = _load_cache()
        client = msal.PublicClientApplication(
            AZURE_CLIENT_ID, authority=AUTHORITY, token_cache=cache
        )
        result = client.acquire_token_by_device_flow(flow)

        if "access_token" in result:
            _save_cache(cache)
            redis_del("device_flow")
            return jsonify(success=True, message="Autenticado correctamente.")

        return jsonify(success=False, details=result)

    except Exception as e:
        app.logger.exception("finish-auth error")
        return jsonify(error=str(e)), 500

@app.get("/auth-status")
def auth_status():
    tok = _acquire_token_silent()
    return jsonify({"authenticated": bool(tok)})

# =====================================================================
# Diagnóstico
# =====================================================================
@app.get("/diag")
def diag():
    return jsonify(ok=True, time=time.time())

@app.get("/debug-flow")
def debug_flow():
    flow = redis_get("device_flow")
    return jsonify({"has_flow": bool(flow), "keys": list(flow.keys()) if flow else None})

# =====================================================================
# UI con botón
# =====================================================================
@app.get("/generar-tarjetas-ui")
def generar_tarjetas_ui():
    return render_template("generar_tarjetas.html")

# =====================================================================
# GET + POST para generar tarjetas
# =====================================================================
@app.route("/generar-tarjetas", methods=["GET", "POST"])
def generar_tarjetas():
    if request.method == "GET":
        return jsonify({
            "status": True,
            "message": "Usa POST para generar las tarjetas."
        })

    try:
        n = generar_tarjetas_y_enviar()
        return jsonify(status=True, generadas=n)
    except Exception as e:
        app.logger.exception("generar-tarjetas error")
        return jsonify(status=False, error=str(e)), 500

# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT","8080")))
