FROM python:3.11-slim

# 1. Cài mấy thư viện cần cho Chrome
RUN apt-get update && \
    apt-get install -y wget gnupg unzip curl && \
    apt-get install -y chromium chromium-driver && \
    apt-get clean

# 2. Tạo thư mục app
WORKDIR /app

# 3. Copy code
COPY . /app

# 4. Cài req
RUN pip install --no-cache-dir -r requirements.txt

# 5. Biến môi trường để chromium chạy đầu không
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# 6. Chạy dạng: python crawler_from_excel.py
CMD ["python", "crawler_from_excel.py"]
