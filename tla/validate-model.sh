#!/usr/bin/env bash

WORKDIR="$(mktemp -d)"
TOOLDIR="${WORKDIR}/tool"
STATEDIR="${WORKDIR}/state"
FAILFAST=false
PARALLEL=$(nproc)

function show_usage {
    echo "usage: validate-model.sh -s <spec> -c <config>">&2
}

function install_tlaplus {
    echo -n "Downloading TLA+ tools ... "
    wget -qN https://nightly.tlapl.us/dist/tla2tools.jar -P ${TOOLDIR}
    wget -qN https://github.com/tlaplus/CommunityModules/releases/latest/download/CommunityModules-deps.jar -P ${TOOLDIR}
    echo "done."
}

function validate {
    local spec=${1}
    local config=${2}
    local tooldir=${3}
    local statedir=${4}

    set -o pipefail
    java -XX:+UseParallelGC -cp ${tooldir}/tla2tools.jar:${tooldir}/CommunityModules-deps.jar tlc2.TLC -config "${config}" "${spec}" -lncheck final -metadir "${statedir}" -fpmem 0.9
}

while getopts :hs:c:p: flag
do
    case "${flag}" in
        s) SPEC=${OPTARG};;
        c) CONFIG=${OPTARG};;
        h|*) show_usage; exit 1;; 
    esac
done

if [ ! "$SPEC" ] || [ ! "$CONFIG" ] 
then
    show_usage
    exit 1
fi

echo "spec: ${SPEC}"
echo "config: ${CONFIG}"

install_tlaplus

validate $SPEC $CONFIG $TOOLDIR $STATEDIR

