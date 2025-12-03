FROM tpcorg/hammerdb:postgres

# Create scripts directory
RUN mkdir -p /scripts

# Copy custom scripts
COPY scripts/pg_tprocc_build.tcl /scripts/
COPY scripts/entrypoint.sh /entrypoint.sh

# Make scripts executable
RUN chmod +x /scripts/*.tcl 2>/dev/null || true
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
