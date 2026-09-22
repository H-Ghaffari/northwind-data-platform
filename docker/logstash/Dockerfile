# ---------------------------------------------------------------------------
# Logstash with a MongoDB input.
#
# There is no official MongoDB input for Logstash. logstash-input-mongodb is
# a community plugin, lightly maintained, and not guaranteed against current
# Logstash or MongoDB releases — which is why it is installed at build time
# and exercised by a smoke test before anything depends on it.
# ---------------------------------------------------------------------------
FROM logstash:9.5.3

RUN bin/logstash-plugin install logstash-input-mongodb