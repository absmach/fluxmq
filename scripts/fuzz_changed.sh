#!/usr/bin/env bash
# Copyright (c) Abstract Machines
# SPDX-License-Identifier: Apache-2.0
#
# Runs the fuzz targets whose package, or any package of this module it depends
# on, changed since BASE. A change to go.mod or go.sum runs every target.
#
#   BASE      git ref to diff against; empty runs every target (default: empty)
#   FUZZTIME  time per target, as for go test -fuzztime (default: 30s)
set -euo pipefail

BASE="${BASE:-}"
FUZZTIME="${FUZZTIME:-30s}"
MODULE="$(go list -m)"

fuzz_packages() {
	grep -rl --include='*_test.go' '^func Fuzz' . | xargs -r dirname | sort -u |
		sed "s#^\.#${MODULE}#"
}

changed_dirs() {
	git diff --name-only "${BASE}...HEAD" -- '*.go' 'go.mod' 'go.sum' | xargs -r -n1 dirname | sort -u |
		sed -e "s#^\.\$#${MODULE}#" -e "s#^\([^/].*\)#${MODULE}/\1#"
}

run_all=false
if [[ -z "${BASE}" ]] || git diff --name-only "${BASE}...HEAD" -- go.mod go.sum | grep -q .; then
	run_all=true
fi
changed="$(${run_all} || changed_dirs)"

status=0
ran=0
for pkg in $(fuzz_packages); do
	if ! ${run_all}; then
		deps="$(go list -deps -test -f '{{.ImportPath}}' "${pkg}" | grep "^${MODULE}" | sed 's/ .*//; s/\.test$//' | sort -u)"
		if ! grep -qxF -f <(printf '%s\n' "${changed}") <<<"${deps}"; then
			continue
		fi
	fi
	dir="${pkg#"${MODULE}"}"
	for target in $(grep -h -o '^func Fuzz[A-Za-z0-9_]*' ".${dir}"/*_test.go | sed 's/^func //'); do
		echo "::group::${pkg} ${target}"
		ran=$((ran + 1))
		if ! go test -run '^$' -fuzz "^${target}\$" -fuzztime "${FUZZTIME}" "${pkg}"; then
			status=1
		fi
		echo "::endgroup::"
	done
done

echo "ran ${ran} fuzz target(s)"
exit "${status}"
