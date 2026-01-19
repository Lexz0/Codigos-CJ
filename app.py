# -*- coding: utf-8 -*-
import os, io, time, json, base64, hashlib, re
from urllib.parse import quote
import requests
import pandas as pd
from flask import Flask, request, jsonify, Response
from PIL import Image, ImageDraw, ImageFont
import msal
from pathlib import Path

# --------------- Config ---------------
BOT_TOKEN = os.environ["BOT_TOKEN"]
GROUP_CHAT_ID = int(os.environ["GROUP_CHAT_ID"])
BARKODER_SECRET = os.environ["BARKODER_SECRET"]

ONEDRIVE_SHARE_LINK = os.environ["ONEDRIVE_SHARE_LINK"]  # 1drv.ms link
EVIDENCIAS_FOLDER = "Evidencias CJ"

ALUMNOS_XLSX_LOCAL = "alumnos.xlsx"
SHEET_ALUMNOS = "Control Asistencia"
SHEET_REGISTROS = "Registros"

FONT_TEXT = os.environ.get("FONT_TEXT", "NotoSans-Regular.ttf")
FONT_CODE39 = os.environ.get("FONT_CODE39", "IDAutomationHC39M.ttf")
CARD_W, CARD_H = 900, 500

CODE_REGEX = re.compile(r"^[A-Za-z0-9\-\_]{3,64}$")

# Azure/MSAL
AZURE_CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
AZURE_TENANT = os.environ.get("AZURE_TENANT", "consumers")
MSAL_CACHE_PATH = os.environ.get("MSAL_CACHE_PATH", "msal_cache.json")
SCOPES = ["User.Read", "Files.ReadWrite", "offline_access"]

AUTHORITY = f"https://login.microsoftonline.com/{AZURE_TENANT}"
GRAPH_ROOT = "https://graph.microsoft.com/v1.0"

app = Flask(__name__)

# --------------- MSAL Helpers ---------------
def _load_cache():
    cache = msal.SerializableTokenCache()
    if os.path.exists(MSAL_CACHE_PATH):
        cache.deserialize(Path(MSAL_CACHE_PATH).read_text())
    return cache

def _save_cache(cache):
    if cache.has_state_changed:
        Path(MSAL_CACHE_PATH).write_text(cache.serialize())

def _acquire_token_interactive():
    cache = _load_cache()
    app_msal = msal.PublicClientApplication(
        client_id=AZURE_CLIENT_ID, authority=AUTHORITY, token_cache=cache
    )
    # Intento silencioso
    accounts = app_msal.get_accounts()
    if accounts:
        result = app_msal.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            _save_cache(cache)
            return result

    # Device Code Flow
    flow = app_msal.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError("No se pudo iniciar device flow.")
    # Devolvemos instrucciones al cliente
    return {"device_flow": flow, "app_msal": app_msal, "cache": cache}

def _acquire_token_by_device_flow(flow, app_msal, cache):
    result = app_msal.acquire_token_by_device_flow(flow)
    if "access_token" in result:
        _save_cache(cache)
    return result

def _get_token_silent():
    cache = _load_cache()
    app_msal = msal.PublicClientApplication(
        client_id=AZURE_CLIENT_ID, authority=AUTHORITY, token_cache=cache
    )
    accounts = app_msal.get_accounts()
    if accounts:
        result = app_msal.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            return result
    return None

# --------------- Graph: resolve share + download/upload ---------------
def _graph_headers(token, extra=None):
    h = {"Authorization": f"Bearer {token}"}
    if extra:
        h.update(extra)
    return h

def _get_token_or_raise():
    result = _get_token_silent()
    if not result or "access_token" not in result:
        raise RuntimeError("No hay token válido. Ejecuta /init-auth.")
    return result["access_token"]

def _resolve_share(token):
    """
    Usa 'Prefer: redeemSharingLink' para resolver el share link a driveItem.
    """
    # Encode del link como /shares/{encoded}/driveItem
    encoded = "u!" + base64.urlsafe_b64encode(ONEDRIVE_SHARE_LINK.encode("utf-8")).decode("utf-8").rstrip("=")
    url = f"{GRAPH_ROOT}/shares/{encoded}/driveItem"
    r = requests.get(url, headers=_graph_headers(token, {"Prefer": "redeemSharingLink"}), timeout=30)
    r.raise_for_status()
    return r.json()  # contiene driveId, id, etc.

def download_excel():
    token = _get_token_or_raise()
    item = _resolve_share(token)
    item_id = item["id"]
    url = f"{GRAPH_ROOT}/drive/items/{item_id}/content"
    r = requests.get(url, headers=_graph_headers(token), timeout=60)
    r.raise_for_status()
    with open(ALUMNOS_XLSX_LOCAL, "wb") as f:
        f.write(r.content)

def upload_excel():
    token = _get_token_or_raise()
    item = _resolve_share(token)
    item_id = item["id"]
    url = f"{GRAPH_ROOT}/drive/items/{item_id}/content"
    with open(ALUMNOS_XLSX_LOCAL, "rb") as f:
        r = requests.put(url, headers=_graph_headers(token), data=f, timeout=120)
    r.raise_for_status()

def upload_image_to_onedrive(filename: str, content: bytes):
    token = _get_token_or_raise()
    # Crear carpeta si no existe (idempotente)
    url_check = f"{GRAPH_ROOT}/me/drive/root:/{quote(EVIDENCIAS_FOLDER)}"
    r = requests.get(url_check, headers=_graph_headers(token), timeout=30)
    if r.status_code == 404:
        # crear
        url_create = f"{GRAPH_ROOT}/me/drive/root/children"
        r2 = requests.post(url_create,
                           headers=_graph_headers(token, {"Content-Type":"application/json"}),
                           json={"name": EVIDENCIAS_FOLDER, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"},
                           timeout=30)
        r2.raise_for_status()
    elif r.status_code != 200:
        r.raise_for_status()

    # Subir archivo
    url_put = f"{GRAPH_ROOT}/me/drive/root:/{quote(EVIDENCIAS_FOLDER)}/{quote(filename)}:/content"
    r3 = requests.put(url_put, headers=_graph_headers(token), data=content, timeout=120)
    r3.raise_for_status()

# --------------- Telegram helpers ---------------
def tg_send_photo(chat_id, png_bytesio, caption=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {"photo": ("tarjeta.png", png_bytesio, "image/png")}
    data = {"chat_id": chat_id, "caption": caption or ""}
    r = requests.post(url, data=data, files=files, timeout=30)
    r.raise_for_status()
    return r.json()

def tg_send_message(chat_id, text, parse_mode=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# --------------- Excel helpers ---------------
def load_df():
    download_excel()
    df = pd.read_excel(ALUMNOS_XLSX_LOCAL, sheet_name=SHEET_ALUMNOS, engine="openpyxl")
    if "Tarjeta generada" not in df.columns:
        df["Tarjeta generada"] = ""
    return df

def save_df_and_append_registro(df, codigo, nombre, clase, fecha, conf_admitido=False):
    if conf_admitido:
        idx = df.index[df["Código"].astype(str).str.strip() == str(codigo).strip()]
        if len(idx) > 0:
            df.loc[idx, "Conf. Bot"] = "Admitido"

    # Registros
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

    upload_excel()

# --------------- Imagen (Code 39) ---------------
def crear_tarjeta(nombre, codigo):
    img = Image.new("RGB", (CARD_W, CARD_H), "white")
    draw = ImageDraw.Draw(img)

    f_name = ImageFont.truetype(FONT_TEXT, size=64)
    f_code_human = ImageFont.truetype(FONT_TEXT, size=36)
    f_code39 = ImageFont.truetype(FONT_CODE39, size=160)

    tw, th = draw.textsize(nombre, font=f_name)
    draw.text(((CARD_W - tw) / 2, 40), nombre, fill="black", font=f_name)

    code_for_bar = f"*{codigo}*"  # Start/Stop para Code 39
    bw, bh = draw.textsize(code_for_bar, font=f_code39)
    y_bar = (CARD_H // 2) - 60
    draw.text(((CARD_W - bw) / 2, y_bar), code_for_bar, fill="black", font=f_code39)

    hw, hh = draw.textsize(codigo, font=f_code_human)
    draw.text(((CARD_W - hw) / 2, y_bar + bh + 10), codigo, fill="#333333", font=f_code_human)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

# --------------- Parte 1: batch tarjetas ---------------
def generar_tarjetas_y_enviar():
    df = load_df()
    hechos = 0
    for i, row in df.iterrows():
        codigo = str(row.get("Código","")).strip()
        if not codigo or not CODE_REGEX.match(codigo):
            continue
        if str(row.get("Tarjeta generada","")).strip():
            continue  # ya generada

        nombre = f"{str(row.get('Nombres','')).strip()} {str(row.get('Apellidos','')).strip()}".strip()
        clase = str(row.get("Clase a la que asiste","")).strip()

        png = crear_tarjeta(nombre, codigo)
        caption = f"{nombre}\nClase: {clase}"

        # Enviar al grupo
        tg_send_photo(GROUP_CHAT_ID, png, caption=caption)

        # Guardar en OneDrive
        safe_name = f"{nombre.replace(' ','_')}_{codigo}.png"
        upload_image_to_onedrive(safe_name, png.getvalue())

        # Marcar 'Tarjeta generada'
        df.at[i, "Tarjeta generada"] = time.strftime("%Y-%m-%d %H:%M:%S")
        hechos += 1

    with pd.ExcelWriter(ALUMNOS_XLSX_LOCAL, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name=SHEET_ALUMNOS, index=False)
    upload_excel()
    return hechos

# --------------- Parte 2: escaneo barkoder ---------------
def procesar_codigo(codigo: str):
    if not CODE_REGEX.match(codigo):
        return False, "Código con formato no válido."

    df = load_df()
    fila = df.loc[df["Código"].astype(str).str.strip() == codigo.strip()]
    if fila.empty:
        return False, "Código no encontrado."

    nombre = f"{fila.iloc[0].get('Nombres','')} {fila.iloc[0].get('Apellidos','')}".strip()
    clase  = str(fila.iloc[0].get("Clase a la que asiste","")).strip()
    juega  = str(fila.iloc[0].get("Juega?","")).strip().lower()
    fecha_hoy = time.strftime("%Y-%m-%d")

    if juega in ("sí","si"):
        save_df_and_append_registro(df, codigo, nombre, clase, fecha_hoy, conf_admitido=True)
        tg_send_message(
            GROUP_CHAT_ID,
            f"✅ {nombre} — {clase} — {fecha_hoy}\nPuede ingresar (tiene al menos una “x” en fechas)."
        )
        return True, f"Registrado: {nombre}"
    else:
        save_df_and_append_registro(df, codigo, nombre, clase, fecha_hoy, conf_admitido=False)
        tg_send_message(
            GROUP_CHAT_ID,
            f"⚠️ {nombre} — {clase} — {fecha_hoy}\nNo tiene asistencias registradas (sin 'x' en fechas)."
        )
        return False, f"Sin asistencia: {nombre}"

# --------------- Rutas ---------------
@app.get("/init-auth")
def init_auth():
    outcome = _acquire_token_interactive()
    if "device_flow" in outcome:
        flow = outcome["device_flow"]
        # Muestra instrucciones del device flow
        return jsonify({
            "message": "Visita la URL e introduce el código para autorizar.",
            "verification_uri": flow.get("verification_uri"),
            "user_code": flow.get("user_code"),
            "expires_in": flow.get("expires_in")
        })
    else:
        return jsonify({"status": "ok", "token": True})

@app.get("/auth-status")
def auth_status():
    tok = _get_token_silent()
    return jsonify({"authenticated": bool(tok)})

@app.post("/generar-tarjetas")
def generar_tarjetas():
    try:
        n = generar_tarjetas_y_enviar()
        return jsonify(status=True, generadas=n)
    except Exception as e:
        return jsonify(status=False, error=str(e)), 500

@app.post("/barkoder-scan")
def barkoder_scan():
    """
    Espera JSON desde Barkoder App:
    {
      "security_data": "1700000000",
      "security_hash": "<md5(security_data + secret)>",
      "data": { "type": "Code39", "value": "ABC123" }   # o base64 del JSON anterior
    }
    """
    body = request.get_json(force=True, silent=True) or {}
    security_data = str(body.get("security_data","")).strip()
    security_hash = str(body.get("security_hash","")).strip()
    data_field = body.get("data")

    if not security_data or not security_hash or data_field is None:
        return jsonify(status=False, message="Parámetros incompletos"), 400

    expected = hashlib.md5((security_data + BARKODER_SECRET).encode("utf-8")).hexdigest()
    if security_hash != expected:
        return jsonify(status=False, message="Hash inválido"), 403

    # Decodificar "data"
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
        return jsonify(status=False, message=f"data inválido: {e}"), 400
        
###cambio No. 1

# PROCESAR data_json DE FORMA ROBUSTA
codigo = None

# 1) Caso: data_json es lista
if isinstance(data_json, list):
    if len(data_json) == 0:
        return jsonify(status=False, message="Barkoder envió una lista vacía en 'data'"), 200
    elemento = data_json[0]
    if isinstance(elemento, dict):
        codigo = (
            elemento.get("value", "") 
            or elemento.get("codevalue", "")
        )
    else:
        return jsonify(status=False, message="El primer elemento de la lista no es un objeto válido"), 200

# 2) Caso: data_json es dict
elif isinstance(data_json, dict):
    codigo = (
        data_json.get("value", "") 
        or data_json.get("codevalue", "")
    )

# 3) Caso: cualquier otro formato
else:
    return jsonify(status=False, message="Formato inesperado en 'data' (no es dict ni lista)"), 200

# 4) Validar que hay código
if not codigo or not str(codigo).strip():
    return jsonify(status=False, message="No se encontró un 'value' válido en Barkoder"), 200

# Normalizar
codigo = str(codigo).strip()


    ok, msg = procesar_codigo(codigo)
    # La app puede mostrar este mensaje (si "Confirmation Feedback" está ON)
    return jsonify(status=bool(ok), message=msg), 200

@app.get("/diag")
def diag():
    return jsonify(ok=True, time=time.time())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
