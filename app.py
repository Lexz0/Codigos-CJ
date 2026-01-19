
# -*- coding: utf-8 -*-
import os, io, time, json, base64, hashlib, re
from urllib.parse import quote
import requests
import pandas as pd
from flask import Flask, request, jsonify
from PIL import Image, ImageDraw, ImageFont
import msal
from pathlib import Path
from urllib.parse import quote as urlquote  # para Upstash REST helpers

# =========================================================
#               Upstash Redis (REST) helpers
#   ¡Estos SÍ guardan/leen el device_flow de forma persistente!
# =========================================================
def redis_set(key, value, ttl_sec=None):
    """
    Upstash REST: POST {URL}/set/<key>/<value>[?EX=<ttl_sec>]
    value se serializa como JSON.
    """
    url = os.environ["REDIS_URL"].rstrip("/")
    token = os.environ["REDIS_TOKEN"]
    val = json.dumps(value)
    qs = f"?EX={int(ttl_sec)}" if ttl_sec else ""
    r = requests.post(
        f"{url}/set/{urlquote(key)}/{urlquote(val)}{qs}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15
    )
    r.raise_for_status()
    return r.json().get("result") == "OK"

def redis_get(key):
    """
    Upstash REST: GET {URL}/get/<key> -> {"result":"<value>"} o {"result":"(nil)"}
    """
    url = os.environ["REDIS_URL"].rstrip("/")
    token = os.environ["REDIS_TOKEN"]
    r = requests.get(
        f"{url}/get/{urlquote(key)}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15
    )
    r.raise_for_status()
    data = r.json()
    val = data.get("result")
    if not val or val == "(nil)":
        return None
    try:
        return json.loads(val)
    except Exception:
        return None

def redis_del(key):
    """
    Upstash REST: POST {URL}/del/<key> -> {"result":1} si borró
    """
    url = os.environ["REDIS_URL"].rstrip("/")
    token = os.environ["REDIS_TOKEN"]
    r = requests.post(
        f"{url}/del/{urlquote(key)}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15
    )
    r.raise_for_status()
    return r.json().get("result") == 1

# =========================================================
#                   CONFIGURACIÓN
# =========================================================
BOT_TOKEN = os.environ["BOT_TOKEN"]
GROUP_CHAT_ID = int(os.environ["GROUP_CHAT_ID"])
BARKODER_SECRET = os.environ["BARKODER_SECRET"]

ONEDRIVE_SHARE_LINK = os.environ["ONEDRIVE_SHARE_LINK"]  # 1drv.ms
EVIDENCIAS_FOLDER = "Evidencias CJ"

ALUMNOS_XLSX_LOCAL = "alumnos.xlsx"
SHEET_ALUMNOS = "Control Asistencia"
SHEET_REGISTROS = "Registros"

FONT_TEXT = os.environ.get("FONT_TEXT", "NotoSans-Regular.ttf")
FONT_CODE39 = os.environ.get("FONT_CODE39", "IDAutomationHC39M.ttf")
CARD_W, CARD_H = 900, 500

# Códigos alfanuméricos con guion y guion_bajo, 3..64 chars
CODE_REGEX = re.compile(r"^[A-Za-z0-9\-\_]{3,64}$")

# MSAL / Graph
AZURE_CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
AZURE_TENANT = os.environ.get("AZURE_TENANT", "consumers")
MSAL_CACHE_PATH = os.environ.get("MSAL_CACHE_PATH", "msal_cache.json")

# Importante: sin 'offline_access' (reservado), solo scopes válidos
SCOPES = ["User.Read", "Files.ReadWrite"]
AUTHORITY = f"https://login.microsoftonline.com/{AZURE_TENANT}"
GRAPH_ROOT = "https://graph.microsoft.com/v1.0"

app = Flask(__name__)

# =========================================================
#                   MSAL helpers
# =========================================================
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
    app_msal = msal.PublicClientApplication(
        AZURE_CLIENT_ID, authority=AUTHORITY, token_cache=cache
    )
    accounts = app_msal.get_accounts()
    if accounts:
        result = app_msal.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            return result
    return None

def _get_token_or_raise():
    tok = _acquire_token_silent()
    if not tok:
        raise RuntimeError("No hay token de Graph. Ejecuta /init-auth y luego /finish-auth.")
    return tok["access_token"]

# =========================================================
#                   OneDrive / Graph
# =========================================================
def _resolve_share(token):
    """
    Resolve de 1drv.ms usando 'redeemSharingLink':
    /shares/u!<base64_urlsafe(share_link)>/driveItem
    """
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

def download_excel():
    token = _get_token_or_raise()
    item = _resolve_share(token)
    item_id = item["id"]

    url = f"{GRAPH_ROOT}/drive/items/{item_id}/content"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    r.raise_for_status()
    with open(ALUMNOS_XLSX_LOCAL, "wb") as f:
        f.write(r.content)

def upload_excel():
    token = _get_token_or_raise()
    item = _resolve_share(token)
    item_id = item["id"]

    url = f"{GRAPH_ROOT}/drive/items/{item_id}/content"
    with open(ALUMNOS_XLSX_LOCAL, "rb") as f:
        r = requests.put(url, headers={"Authorization": f"Bearer {token}"}, data=f, timeout=120)
    r.raise_for_status()

def upload_image_to_onedrive(filename, content):
    token = _get_token_or_raise()

    # Crear carpeta si no existe
    url_check = f"{GRAPH_ROOT}/me/drive/root:/{quote(EVIDENCIAS_FOLDER)}"
    r = requests.get(url_check, headers={"Authorization": f"Bearer {token}"}, timeout=30)

    if r.status_code == 404:
        url_create = f"{GRAPH_ROOT}/me/drive/root/children"
        r2 = requests.post(
            url_create,
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

# =========================================================
#                   Telegram helpers
# =========================================================
def tg_send_photo(chat_id, png_bytesio, caption=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {"photo": ("tarjeta.png", png_bytesio, "image/png")}
    data = {"chat_id": chat_id, "caption": caption or ""}
    r = requests.post(url, data=data, files=files, timeout=30)
    r.raise_for_status()
    return r.json()

def tg_send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=30)
    r.raise_for_status()
    return r.json()

# =========================================================
#                   Excel helpers
# =========================================================
def load_df():
    download_excel()
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
        regs = pd.DataFrame(columns=["timestamp", "codigo", "nombre", "clase", "fecha"])

    regs = pd.concat([
        regs,
        pd.DataFrame([{
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "codigo": codigo,
            "nombre": nombre,
            "clase": clase,
            "fecha": fecha
        }])
    ], ignore_index=True)

    with pd.ExcelWriter(ALUMNOS_XLSX_LOCAL, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name=SHEET_ALUMNOS, index=False)
        regs.to_excel(w, sheet_name=SHEET_REGISTROS, index=False)

    upload_excel()

# =========================================================
#                   Imagen (tarjeta)
# =========================================================
def crear_tarjeta(nombre, codigo):
    img = Image.new("RGB", (CARD_W, CARD_H), "white")
    draw = ImageDraw.Draw(img)

    f_name = ImageFont.truetype(FONT_TEXT, size=64)
    f_code39 = ImageFont.truetype(FONT_CODE39, size=160)
    f_human = ImageFont.truetype(FONT_TEXT, size=36)

    # Nombre
    tw, th = draw.textsize(nombre, font=f_name)
    draw.text(((CARD_W - tw)/2, 40), nombre, fill="black", font=f_name)

    # Code39 (con *start/stop*)
    code_for_bar = f"*{codigo}*"
    bw, bh = draw.textsize(code_for_bar, font=f_code39)
    y = (CARD_H // 2) - 60
    draw.text(((CARD_W - bw)/2, y), code_for_bar, fill="black", font=f_code39)

    # Texto legible
    hw, hh = draw.textsize(codigo, font=f_human)
    draw.text(((CARD_W - hw)/2, y + bh + 10), codigo, fill="#333333", font=f_human)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

# =========================================================
#                   Parte 1: tarjetas batch
# =========================================================
def generar_tarjetas_y_enviar():
    df = load_df()
    hechos = 0

    for i, row in df.iterrows():
        codigo = str(row.get("Código", "")).strip()
        if not codigo or not CODE_REGEX.match(codigo):
            continue

        # Evitar duplicados
        if str(row.get("Tarjeta generada", "")).strip():
            continue

        nombre = f"{str(row.get('Nombres','')).strip()} {str(row.get('Apellidos','')).strip()}".strip()
        clase = str(row.get("Clase a la que asiste", "")).strip()

        png = crear_tarjeta(nombre, codigo)
        tg_send_photo(GROUP_CHAT_ID, png, f"{nombre}\nClase: {clase}")
        upload_image_to_onedrive(f"{nombre.replace(' ','_')}_{codigo}.png", png.getvalue())

        df.at[i, "Tarjeta generada"] = time.strftime("%Y-%m-%d %H:%M:%S")
        hechos += 1

    with pd.ExcelWriter(ALUMNOS_XLSX_LOCAL, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name=SHEET_ALUMNOS, index=False)

    upload_excel()
    return hechos

# =========================================================
#                   Parte 2: escaneo Barkoder
# =========================================================
def procesar_codigo(codigo: str):
    if not CODE_REGEX.match(codigo):
        return False, "Código inválido"

    df = load_df()
    fila = df.loc[df["Código"].astype(str).str.strip() == codigo]
    if fila.empty:
        return False, "Código no encontrado"

    nombre = f"{fila.iloc[0].get('Nombres','')} {fila.iloc[0].get('Apellidos','')}".strip()
    clase  = str(fila.iloc[0].get("Clase a la que asiste", "")).strip()
    juega  = str(fila.iloc[0].get("Juega?", "")).strip().lower()
    fecha  = time.strftime("%Y-%m-%d")

    if juega in ("sí", "si"):
        save_df_and_append_registro(df, codigo, nombre, clase, fecha, conf=True)
        tg_send_message(GROUP_CHAT_ID, f"✅ {nombre} — {clase} — {fecha}\nPuede ingresar.")
        return True, f"Registrado: {nombre}"
    else:
        save_df_and_append_registro(df, codigo, nombre, clase, fecha, conf=False)
        tg_send_message(GROUP_CHAT_ID, f"⚠️ {nombre} — {clase} — {fecha}\nNO tiene asistencias registradas.")
        return False, "Sin asistencia"

# =========================================================
#                   Endpoints
# =========================================================
@app.post("/barkoder-scan")
def barkoder_scan():
    """
    Recibe POST de la app Barkoder:
    {
      "security_data": "1700000000",
      "security_hash": "<md5(security_data + secret)>",
      "data": { "type": "Code39", "value": "ABC123" }  # o una lista, o base64
    }
    """
    try:
        body = request.get_json(force=True, silent=True) or {}
        security_data = str(body.get("security_data", "")).strip()
        security_hash = str(body.get("security_hash", "")).strip()
        data_field = body.get("data")

        if not security_data or not security_hash or data_field is None:
            return jsonify(status=False, message="Parámetros incompletos"), 200

        expected = hashlib.md5((security_data + BARKODER_SECRET).encode("utf-8")).hexdigest()
        if security_hash != expected:
            return jsonify(status=False, message="Hash inválido"), 200

        # Decodificar data
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

        # Extraer el código robustamente (dict o list)
        codigo = None
        if isinstance(data_json, list):
            if len(data_json) == 0:
                return jsonify(status=False, message="Lista vacía"), 200
            elem = data_json[0]
            if isinstance(elem, dict):
                codigo = elem.get("value", "") or elem.get("codevalue", "")
            else:
                return jsonify(status=False, message="Elemento inválido en data"), 200
        elif isinstance(data_json, dict):
            codigo = data_json.get("value", "") or data_json.get("codevalue", "")
        else:
            return jsonify(status=False, message="Formato inesperado en data"), 200

        if not codigo or not str(codigo).strip():
            return jsonify(status=False, message="No se encontró 'value'"), 200

        codigo = str(codigo).strip()

        ok, msg = procesar_codigo(codigo)
        return jsonify(status=bool(ok), message=msg), 200

    except Exception as e:
        app.logger.exception("Error en barkoder-scan")
        return jsonify(status=False, message=f"Excepción: {e}"), 200

# ------------------- Auth: iniciar Device Flow -------------------
@app.get("/init-auth")
def init_auth_fixed():
    try:
        cache = _load_cache()
        app_msal = msal.PublicClientApplication(
            AZURE_CLIENT_ID, authority=AUTHORITY, token_cache=cache
        )

        flow = app_msal.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            return jsonify(error="No se pudo iniciar device flow"), 500

        # Guardar flow en Redis con TTL (expires_in)
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

# ------------------- Auth: completar Device Flow -----------------
@app.get("/finish-auth")
def finish_auth():
    try:
        flow = redis_get("device_flow")
        if not flow:
            return jsonify(error="No hay flow pendiente. Ejecuta /init-auth primero."), 400

        cache = _load_cache()
        app_msal = msal.PublicClientApplication(
            AZURE_CLIENT_ID, authority=AUTHORITY, token_cache=cache
        )

        result = app_msal.acquire_token_by_device_flow(flow)

        if "access_token" in result:
            _save_cache(cache)
            redis_del("device_flow")
            return jsonify(success=True, message="Autenticado correctamente.")

        # Si aún está pendiente o expirado, devolvemos detalle
        return jsonify(success=False, details=result)
    except Exception as e:
        app.logger.exception("finish-auth error")
        return jsonify(error=str(e)), 500

# ------------------- Auth: estado -------------------
@app.get("/auth-status")
def auth_status():
    tok = _acquire_token_silent()
    return jsonify({"authenticated": bool(tok)})

# ------------------- Batch tarjetas -------------------
@app.post("/generar-tarjetas")
def generar_tarjetas():
    try:
        n = generar_tarjetas_y_enviar()
        return jsonify(status=True, generadas=n)
    except Exception as e:
        app.logger.exception("generar-tarjetas error")
        return jsonify(status=False, error=str(e)), 500

# ------------------- Diagnóstico -------------------
@app.get("/diag")
def diag():
    return jsonify(ok=True, time=time.time())

# (Opcional) Depuración rápida del flow en Redis
@app.get("/debug-flow")
def debug_flow():
    flow = redis_get("device_flow")
    return jsonify({
        "has_flow": bool(flow),
        "keys": list(flow.keys()) if flow else None
    })

# =========================================================
#                   MAIN
# =========================================================
if __name__ == "__main__":
    # Render setea PORT; gunicorn lo usará en producción.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
