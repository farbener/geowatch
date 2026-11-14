#!/bin/sh
# Pull new code from GitHub and restart Geowatch only if something changed.
# Run as root by geowatch-update.timer; git itself runs as the geowatch user that owns the files.
set -e
cd /opt/geowatch
before=$(runuser -u geowatch -- git rev-parse HEAD)
runuser -u geowatch -- git pull --ff-only --quiet
after=$(runuser -u geowatch -- git rev-parse HEAD)
if [ "$before" != "$after" ]; then
    echo "updated $before -> $after, restarting"
    systemctl restart geowatch
fi
