# ---------------------------------------------------------------------------
# Grafana with the ClickHouse datasource baked in.
#
# The plugin is unpacked from a vendored archive at build time rather than
# downloaded when the container starts. Startup-time installation makes the
# service depend on reaching a CDN that refuses requests from some regions,
# and Grafana treats a failed plugin install as fatal — so the container
# exits, restarts, fails again, and never comes up.
#
# Vendoring the archive costs a few megabytes in the repository and buys an
# image that starts identically on any machine, including one with no
# internet access at all.
#
# Plugins live outside /var/lib/grafana so the named volume mounted there
# cannot shadow them.
# ---------------------------------------------------------------------------
FROM grafana/grafana:11.6.0

USER root

ENV GF_PATHS_PLUGINS=/opt/grafana/plugins

COPY plugins/clickhouse-datasource.zip /tmp/plugin.zip

RUN mkdir -p ${GF_PATHS_PLUGINS} \
 && unzip -q /tmp/plugin.zip -d ${GF_PATHS_PLUGINS} \
 && rm /tmp/plugin.zip \
 && chown -R 472:0 ${GF_PATHS_PLUGINS} \
 && chmod -R 755 ${GF_PATHS_PLUGINS}

USER grafana
