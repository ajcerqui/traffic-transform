FROM postgres:14

# Install cron
RUN apt-get update && apt-get -y install cron && apt-get clean

# Copy transformation script
COPY transform.sql /usr/local/bin/transform.sql

# Create transformation shell script
RUN echo '#!/bin/bash\n\
echo "Starting transformation at $(date)"\n\
psql $DATABASE_URL -f /usr/local/bin/transform.sql\n\
if [ $? -eq 0 ]; then\n\
    echo "Transformation completed successfully at $(date)"\n\
else\n\
    echo "Transformation failed at $(date)"\n\
fi' > /usr/local/bin/run_transform.sh

# Make the script executable
RUN chmod +x /usr/local/bin/run_transform.sh

# Create crontab file - run every hour, 15 minutes after the hour 
# (to ensure ingestion has completed)
RUN echo "15 * * * * /usr/local/bin/run_transform.sh >> /var/log/cron.log 2>&1" > /etc/cron.d/transform-cron
RUN chmod 0644 /etc/cron.d/transform-cron

# Create log file
RUN touch /var/log/cron.log

# Create entrypoint script
RUN echo '#!/bin/bash\n\
env >> /etc/environment\n\
cron\n\
echo "Transformation service started. Watching log..."\n\
tail -f /var/log/cron.log' > /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]
