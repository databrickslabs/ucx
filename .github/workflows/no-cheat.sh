#!/bin/sh

NEW_CODE=$(git diff origin/main..$(git branch --show-current) | grep -e '^+')
CHEAT=$(echo "${NEW_CODE}" | grep '# pylint: disable')
if [ -n "${CHEAT}" ]; then
  echo "Do not cheat the linter: ${CHEAT}"
  exit 1
fi