# Imagen base de Python
FROM python:3.11-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el script
COPY ws_vivienda.py .

# Instalar dependencias
RUN pip install --no-cache-dir \
    pandas \
    requests \
    beautifulsoup4 \
    tqdm

# Crear carpeta de logs y datos
RUN mkdir -p /app/logs /app/data

# Correr el script
CMD ["python", "ws_vivienda.py"]

