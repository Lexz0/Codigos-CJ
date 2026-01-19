# -*- coding: utf-8 -*-
import os, io, time, json, hashlib, base64, re
import requests
import pandas as pd
from flask import Flask, request, jsonify
from PIL import Image, ImageDraw, ImageFont

# ========= Config =========
BOT_TOKEN = os.environ["BOT_TOKEN"]
GROUP_CHAT_ID = int(os.environ["GROUP_CHAT_ID"])  # -1003580166175
BARKODER_SECRET = os.environ["BARKODER_SECRET"]
ONEDRIVE_SHARE_LINK = os.environ.get("ONEDRIVE_SHARE_LINK")  # 1drv.ms/...

ALUMNOS_XLSX_LOCAL = "alumnos.xlsx"
SHEET_ALUMNOS = "Control Asistencia"
SHEET_REGISTROS = "Registros"
EVIDENCIAS_FOLDER = "Evidencias CJ"  # carpeta en OneDrive

FONT_TEXT = os.environ.get("FONT_TEXT", "NotoSans-Regular.ttf")
FONT_CODE39 = os.environ.get("FONT_CODE39", "IDAutomationHC39M.ttf")
CARD_W, CARD_H = 900, 500

# Un patrón flexible para códigos (ajústalo a tu formato)
CODE_REGEX = re.compile(r"^[A-Za-z0-9\-\_]{3,64}$")

app = Flask(__name__)

# ========= Helpers: Telegram =========
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

# ========= Helpers: OneDrive (stubs) =========
def download_excel():
    """
    Descarga el Excel desde OneDrive con tu flujo MS Graph (redeemSharingLink).
    Deja el archivo local en ALUMNOS_XLSX_LOCAL.
    """
    # TODO: Implementar con tus endpoints MS Graph, como en tu proyecto anterior:
    # 1) POST /shares/{shareIdOrUrl}/driveItem  con Prefer: redeemSharingLink
    # 2) GET  /drive/items/{itemId}/content  -> guardar como alumnos.xlsx
    if not os.path.exists(ALUMNOS_XLSX_LOCAL):
        raise FileNotFoundError("alumnos.xlsx no está disponible localmente.")

def upload_excel():
    """
    Sube el Excel actualizado a OneDrive (misma ruta) con Graph.
    """
    # TODO: Implementar PUT /drive/items/{itemId}/content con tu token MSAL.
    pass

def upload_image_to_onedrive(filename: str, content: bytes):
    """
    Sube el PNG a OneDrive: /Evidencias CJ/<filename>
    """
    # TODO: Implementar PUT /drive/root:/Evidencias CJ/<filename>:/content
    pass

# ========= Helpers: Excel =========
def load_df():
    download_excel()
    df = pd.read_excel(ALUMNOS_XLSX_LOCAL, sheet_name=SHEET_ALUMNOS, engine="openpyxl")
    # Asegurar columna 'Tarjeta generada'
    if "Tarjeta generada" not in df.columns:
        df["Tarjeta generada"] = ""
    return df

def save_df_and_append_registro(df, codigo, nombre, clase, fecha, conf_admitido=False):
    # Marcar 'Admitido' si aplica
    if conf_admitido:
        idx = df.index[df["Código"].astype(str).str.strip() == str(codigo).strip()]
        if len(idx) > 0:
            df.loc[idx, "Conf. Bot"] = "Admitido"

    # Hoja de registros
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

# ========= Imagen (Code 39 con tu fuente) =========
def crear_tarjeta(nombre, codigo):
    """
    Dibuja:
      - Nombre centrado
      - Banda Code39 (con *CODIGO* para start/stop)
      - Texto legible CODIGO
    """
    img = Image.new("RGB", (CARD_W, CARD_H), "white")
    draw = ImageDraw.Draw(img)

    f_name = ImageFont.truetype(FONT_TEXT, size=64)
    f_code_human = ImageFont.truetype(FONT_TEXT, size=36)
    f_code39 = ImageFont.truetype(FONT_CODE39, size=160)

    tw, th = draw.textsize(nombre, font=f_name)
    draw.text(((CARD_W - tw) / 2, 40), nombre, fill="black", font=f_name)

    code_for_bar = f"*{codigo}*"
    bw, bh = draw.textsize(code_for_bar, font=f_code39)
    y_bar = (CARD_H // 2) - 60
    draw.text(((CARD_W - bw) / 2, y_bar), code_for_bar, fill="black", font=f_code39)

    hw, hh = draw.textsize(codigo, font=f_code_human)
    draw.text(((CARD_W - hw) / 2, y_bar + bh + 10), codigo, fill="#333333", font=f_code_human)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

# ========= Lógica batch (Parte 1) =========
def generar_tarjetas_y_enviar():
    df = load_df()
    hechos = 0

    for i, row in df.iterrows():
        codigo = str(row.get("Código","")).strip()
        if not codigo or not CODE_REGEX.match(codigo):
            continue

        # Evitar duplicados
        if str(row.get("Tarjeta generada","")).strip():
            continue

        nombre = f"{str(row.get('Nombres','')).strip()} {str(row.get('Apellidos','')).strip()}".strip()
        clase = str(row.get("Clase a la que asiste","")).strip()

        # Crear imagen
        png = crear_tarjeta(nombre, codigo)
        caption = f"{nombre}\nClase: {clase}"

        # Enviar al grupo (Telegram)
        tg_send_photo(GROUP_CHAT_ID, png, caption=caption)

        # Guardar en OneDrive
        safe_name = f"{nombre.replace(' ','_')}_{codigo}.png"
        upload_image_to_onedrive(safe_name, png.getvalue())

        # Marcar 'Tarjeta generada'
        df.at[i, "Tarjeta generada"] = time.strftime("%Y-%m-%d %H:%M:%S")
        hechos += 1

    # Persistir cambios en Excel
    with pd.ExcelWriter(ALUMNOS_XLSX_LOCAL, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
        df.to_excel(w, sheet_name=SHEET_ALUMNOS, index=False)
    upload_excel()

    return hechos

# ========= Lógica de escaneo (Parte 2) =========
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
        # Marca Admitido + registra
        save_df_and_append_registro(df, codigo, nombre, clase, fecha_hoy, conf_admitido=True)
        tg_send_message(
            GROUP_CHAT_ID,
            f"✅ {nombre} — {clase} — {fecha_hoy}\nPuede ingresar (tiene al menos una “x” en fechas)."
        )
        return True, f"Registrado: {nombre}"
    else:
        # Solo informar; no marcar
        save_df_and_append_registro(df, codigo, nombre, clase, fecha_hoy, conf_admitido=False)
        tg_send_message(
            GROUP_CHAT_ID,
            f"⚠️ {nombre} — {clase} — {fecha_hoy}\nNo tiene asistencias registradas (sin 'x' en fechas)."
        )
        return False, f"Sin asistencia: {nombre}"

# ========= Endpoints =========
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
    Espera JSON desde la app Barkoder:
    {
      "security_data": "1700000000",
      "security_hash": "<md5(security_data + secret)>",
      "data": { "type": "Code39", "value": "ABC123" }   # o base64 si 'encode data' ON
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
            # Puede venir base64 o JSON string
            try:
                decoded = base64.b64decode(data_field)
                data_json = json.loads(decoded.decode("utf-8"))
            except Exception:
                data_json = json.loads(data_field)
        else:
            data_json = data_field
    except Exception as e:
        return jsonify(status=False, message=f"data inválido: {e}"), 400

    codigo = str(data_json.get("value","") or data_json.get("codevalue","")).strip()
    if not codigo:
        return jsonify(status=False, message="No se encontró 'value'"), 400

    ok, msg = procesar_codigo(codigo)
    # La app Barkoder puede mostrar este message si activas "Confirmation Feedback"
    return jsonify(status=bool(ok), message=msg), (200 if ok else 200)

@app.get("/diag")
def diag():
    return jsonify(ok=True, time=time.time())
