FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN apt-get update && apt-get install -y cron

# Create cron job to run the batch script daily at 2:30 AM UTC
RUN echo "30 2 * * * python -m app.market_data.daily_market_price_batch >> /var/log/batch.log 2>&1" | crontab -

RUN touch /var/log/batch.log

COPY start.sh /start.sh
RUN chmod +x /start.sh
CMD ["/start.sh"]
