import streamlit as st
import pandas as pd
from google.cloud import storage
import io
from datetime import datetime
import zipfile

# -------------------------------------------------------
# 1. Conexión al bucket y detección del archivo más reciente
# -------------------------------------------------------

client = storage.Client.from_service_account_info(st.secrets["gcp_service_account"])

bucket_name = "bk_proactive"
blobs = list(client.list_blobs(bucket_name))

# Filtrar solo archivos que terminen con _SmartProactive.csv
archivos = [b for b in blobs if b.name.endswith("_SmartProactive.csv")]

if not archivos:
    st.error("No se encontró ningún archivo con el patrón *_SmartProactive.csv en el bucket.")
    st.stop()

# Seleccionar el último modificado
blob = max(archivos, key=lambda x: x.updated)
file_name = blob.name

# Descargar contenido
data = blob.download_as_bytes()

# -------------------------------------------------------
# 2. Obtener fechas de proceso
# -------------------------------------------------------

fecha_metadata = blob.updated.strftime("%Y-%m-%d %H:%M:%S")

try:
    partes = file_name.split("_")
    fecha_nombre = datetime.strptime(partes[0] + partes[1], "%Y%m").strftime("%B %Y")
except Exception:
    fecha_nombre = "No disponible"

# -------------------------------------------------------
# 3. Cargar DataFrame
# -------------------------------------------------------

df = pd.read_csv(io.BytesIO(data), parse_dates=["Date_Contacto1", "Date_Contacto2"])

# -------------------------------------------------------
# 4. Interfaz de usuario
# -------------------------------------------------------

st.title("🚗 Smart Proactive")

st.info(f"📄 Archivo procesado: **{file_name}**")  
st.info(f"📅 Fecha de proceso : **{fecha_nombre}**")  
st.caption(f"(Última modificación en bucket: {fecha_metadata})")

st.markdown("Selecciona el rango de fechas para filtrar los contactos:")

col1, col2 = st.columns(2)
with col1:
    fecha_inicial = st.date_input("Fecha inicial", value=None)
with col2:
    fecha_final = st.date_input("Fecha final", value=None)

# -------------------------------------------------------
# 5. Filtrado y bloques
# -------------------------------------------------------

if fecha_inicial and fecha_final:
    # Bloque 1: primer contacto
    bloque1 = df[
        (df["Date_Contacto1"] >= pd.to_datetime(fecha_inicial)) &
        (df["Date_Contacto1"] <= pd.to_datetime(fecha_final))
    ].copy()
    bloque1["Contacto"] = "1er_Contacto"
    bloque1["Fecha_Contacto"] = bloque1["Date_Contacto1"]
    bloque1["Origen"] = bloque1["Origen_Contacto1"]

    # Bloque 2: segundo contacto
    bloque2 = df[
        (df["Date_Contacto2"] >= pd.to_datetime(fecha_inicial)) &
        (df["Date_Contacto2"] <= pd.to_datetime(fecha_final))
    ].copy()
    bloque2["Contacto"] = "2do_Contacto"
    bloque2["Fecha_Contacto"] = bloque2["Date_Contacto2"]
    bloque2["Origen"] = bloque2["Origen_Contacto2"]

    # Unión
    columnas_comunes = list(set(bloque1.columns).intersection(set(bloque2.columns)))
    resultado = pd.concat([bloque1[columnas_comunes], bloque2[columnas_comunes]], ignore_index=True)
    resultado = resultado.drop_duplicates().reset_index(drop=True)

    # Tratamiento de la Variable Nombre
    resultado["Nombre.Contacto"] = resultado.apply(
        lambda x: x["Nombre.Titular2"] if str(x["Categ"]) in ["2", "E"] else x["Nombre.Titular"],
        axis=1)

    # Orden de columnas
    columnas_deseadas = [
        'Fecha_Contacto', 'Contacto', 'Origen', 'Departamento','Categ', 'Cuenta.Titu',
        'Nombre.Contacto', 'Matricula', 'VIN', 'Name_Family',
        'Año', 'Color.1', 'E.mail', 'Movil', 'Km_ultimo', 'visitas',
        'ultimo_desc_mantenimiento', 'Fec_ultimo_mantenimiento',
        'Km_ult_mtto', 'Km_proyectado', 'Km_comercial', 'next_mtto'
    ]
    resultado = resultado[columnas_deseadas].copy()

    st.success(f"Datos filtrados: {len(resultado)} registros encontrados.")
    st.dataframe(resultado.head(20))

    # -------------------------------------------------------
    # 6. Subconjuntos y descarga ZIP
    # -------------------------------------------------------
    def generar_subset(df, dept, origen, contacto, cols):
        return df[
            (df["Departamento"].isin(dept)) &
            (df["Origen"] == origen) &
            (df["Contacto"].isin(contacto))
        ][cols]

    subsets = {
        "LP_Days_1er.xlsx": generar_subset(
            resultado, ["La Paz", "Oruro"], "Days", ["1er_Contacto", "Ambas"],
            ["Nombre.Contacto", "next_mtto", "Name_Family", "Matricula", "Km_ult_mtto"]
        ),
        "LP_Days_2do.xlsx": generar_subset(
            resultado, ["La Paz", "Oruro"], "Days", ["2do_Contacto"],
            ["Nombre.Contacto", "next_mtto", "Name_Family", "Matricula", "Km_ult_mtto"]
        ),
        "LP_Km_1er.xlsx": generar_subset(
            resultado, ["La Paz", "Oruro"], "Km", ["1er_Contacto", "Ambas"],
            ["Nombre.Contacto", "Matricula", "Km_ult_mtto", "ultimo_desc_mantenimiento", "Km_comercial", "next_mtto"]
        ),
        "LP_Km_2do.xlsx": generar_subset(
            resultado, ["La Paz", "Oruro"], "Km", ["2do_Contacto"],
            ["Nombre.Contacto", "Matricula", "Km_ult_mtto", "ultimo_desc_mantenimiento", "Km_comercial", "next_mtto"]
        ),
        "Cbba_Days_1er.xlsx": generar_subset(
            resultado, ["Cochabamba"], "Days", ["1er_Contacto", "Ambas"],
            ["Nombre.Contacto", "next_mtto", "Name_Family", "Matricula", "Km_ult_mtto"]
        ),
        "Cbba_Days_2do.xlsx": generar_subset(
            resultado, ["Cochabamba"], "Days", ["2do_Contacto"],
            ["Nombre.Contacto", "next_mtto", "Name_Family", "Matricula", "Km_ult_mtto"]
        ),
        "Cbba_Km_1er.xlsx": generar_subset(
            resultado, ["Cochabamba"], "Km", ["1er_Contacto", "Ambas"],
            ["Nombre.Contacto", "Matricula", "Km_ult_mtto", "ultimo_desc_mantenimiento", "Km_comercial", "next_mtto"]
        ),
        "Cbba_Km_2do.xlsx": generar_subset(
            resultado, ["Cochabamba"], "Km", ["2do_Contacto"],
            ["Nombre.Contacto", "Matricula", "Km_ult_mtto", "ultimo_desc_mantenimiento", "Km_comercial", "next_mtto"]
        )
    }

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for fname, df_sub in subsets.items():
            if not df_sub.empty:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
                    df_sub.to_excel(writer, index=False, sheet_name="Datos")
                zipf.writestr(fname, excel_buffer.getvalue())

    zip_buffer.seek(0)

    st.download_button(
        "⬇️ Download Templates",
        data=zip_buffer,
        file_name=f"Proactive_Templates_{fecha_nombre}.zip",
        mime="application/zip"
    )

    # -------------------------------------------------------
    # 7b. Botón para descargar todo el dataframe resultado
    # -------------------------------------------------------
    def to_excel_bytes(df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Resultado")
        return output.getvalue()

    st.download_button(
        "⬇️ Descargar todo el resultado en Excel",
        data=to_excel_bytes(resultado),
        file_name=f"Smart_Proactive_{fecha_nombre}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# -------------------------------------------------------
# 8. Búsqueda individual
# -------------------------------------------------------

st.markdown("### 🔍 Búsqueda individual por Cuenta.Titu o Matrícula")

col1, col2 = st.columns(2)
with col1:
    input_cuenta = st.text_input("Ingrese Cuenta.Titu")
with col2:
    input_matricula = st.text_input("Ingrese Matrícula")

df_busqueda = pd.DataFrame()

if input_cuenta:
    df_busqueda = df[df["Cuenta.Titu"].astype(str) == input_cuenta]

if input_matricula:
    df_busqueda = pd.concat([
        df_busqueda,
        df[df["Matricula"].astype(str) == input_matricula]
    ], ignore_index=True)

df_busqueda = df_busqueda.drop_duplicates().reset_index(drop=True)

if not df_busqueda.empty:
    st.success(f"{len(df_busqueda)} registro(s) encontrados:")
    for idx, row in df_busqueda.iterrows():
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Cuenta.Titu:** {row['Cuenta.Titu']}")
            st.markdown(f"**Nombre:** {row.get('Nombre.Contacto', row.get('Nombre.Titular',''))}")
            st.markdown(f"**Email:** {row.get('E.mail','')}")
            st.markdown(f"**Móvil:** {row.get('Movil','')}")
            st.markdown(f"**Departamento:** {row.get('Departamento','')}")
            st.markdown(f"**Matrícula:** {row['Matricula']}")
            st.markdown(f"**VIN:** {row['VIN']}")
            st.markdown(f"**Name_Family:** {row['Name_Family']}")
            st.markdown(f"**Año Modelo:** {row['Año']}")

        with col2:
            st.markdown(f"**Visitas:** {row['visitas']}")
            st.markdown(f"**Fecha Ultimo Mtto.:** {row.get('Fec_ultimo_mantenimiento','')}")
            st.markdown(f"**Ultimo Mtto.:** {row.get('ultimo_desc_mantenimiento','')}")
            st.markdown(f"**Km Ultimo Mtto.:** {row.get('Km_ult_mtto','')}")
            st.markdown(f"**Siguiente Mantenimiento:** {row.get('next_mtto')}")
            st.markdown(f"**Km Último:** {row.get('Km_ultimo','')}")
            st.markdown(f"**Km Proyectado:** {row.get('Km_proyectado','')}")

else:
    st.info("No se encontraron registros para los valores ingresados.")

# -------------------------------------------------------
# 9. Subida de archivo al bucket
# -------------------------------------------------------
st.markdown("### 🗂 Resultados")

uploaded_file = st.file_uploader("Sube tu archivo XLSX", type=["xlsx"])

if uploaded_file is not None:
    data_bytes = uploaded_file.read()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"results/{timestamp}_{uploaded_file.name}"
