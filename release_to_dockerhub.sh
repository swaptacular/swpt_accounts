#!/bin/sh

if [ -z "$1" ]; then
    echo "Usage: release.sh TAG"
    return
fi

swpt_accounts="epandurski/swpt_accounts:$1"
docker build -t "$swpt_accounts" --target app-image .
git tag "v$1"
git push origin "v$1"
docker login
docker push "$swpt_accounts"
