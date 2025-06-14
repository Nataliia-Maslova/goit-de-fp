FROM apache/spark:3.4.1

# Встановити pip і Python-залежності
USER root
RUN apt-get update && apt-get install -y python3-pip wget

RUN ln -s /usr/bin/python3 /usr/bin/python

# Створення робочої директорії
WORKDIR /app

# Копіюємо всі локальні файли в контейнер
COPY . /app

# Встановлюємо залежності
RUN pip install --no-cache-dir -r requirements.txt

# Додаткові змінні середовища
ENV PYTHONUNBUFFERED=1
ENV PREFECT_API_URL="http://host.docker.internal:4200/api"

CMD ["python", "project_solution.py"]

