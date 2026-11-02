# The PostgreSQL major version must match the db service so pg_dump and pg_restore
# understand the server and the archives it produces.
FROM postgres:16

RUN apt-get update \
    && apt-get install --yes --no-install-recommends age \
    && rm -rf /var/lib/apt/lists/*

COPY --chmod=0755 backup.sh /usr/local/bin/backup-now
COPY --chmod=0755 schedule.sh /usr/local/bin/backup-schedule
COPY --chmod=0755 restore.sh /usr/local/bin/restore-backup
COPY --chmod=0755 row_counts.sh /usr/local/bin/db-row-counts
COPY --chmod=0755 healthcheck.sh /usr/local/bin/backup-healthcheck

ENTRYPOINT []
CMD ["backup-schedule"]
