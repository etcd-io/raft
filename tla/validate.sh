#!/usr/bin/env bash

WORKDIR="$(mktemp -d)"
TOOLDIR="${WORKDIR}/tool"
STATEDIR="${WORKDIR}/state"
FAILFAST=false
PARALLEL=$(nproc)

function show_usage {
    echo "usage: validate.sh [-p <parallel>] -s <spec>-c <config> <trace files>">&2
}

function install_tlaplus {
    echo -n "Downloading TLA+ tools ... "
    wget -qN https://nightly.tlapl.us/dist/tla2tools.jar -P ${TOOLDIR}
    wget -qN https://github.com/tlaplus/CommunityModules/releases/latest/download/CommunityModules-deps.jar -P ${TOOLDIR}
    echo "done."
}

function preprocess_log {
    local trace=${1}
    sed -i -E 's/^[^{]+//' ${trace}
    sort -t":" -k 3 ${trace} -o ${trace}
}

function colored_text {
    color=$1
    text=$2

    case $color in
        "red")
            echo $"\033[0;31m$text\033[0m"
        ;;

        "green")
            echo "\033[0;32m$text\033[0m"
        ;;

        *)
            echo text
        ;;
    esac
}

function validate {
    local id=${1}
    local trace=${2}
    local spec=${3}
    local config=${4}
    local tooldir=${5}
    local statedir=${6}
    local name=$(basename $trace .ndjson)

    # preprocess log
    sed -i -E 's/^[^{]+//' ${trace}
    sort -t":" -k 3 ${trace} -o ${trace}

    set -o pipefail
    env JSON="${trace}" java -XX:+UseParallelGC -cp ${tooldir}/tla2tools.jar:${tooldir}/CommunityModules-deps.jar tlc2.TLC -config "${config}" "${spec}" -lncheck final -metadir "${statedir}" -fpmem 0.9  | sed -nuE "s/<<\"Progress %:\", ([0-9]+)>>$/${id} \1/p"

    if [ "$?" -ne 0 ]; then
        echo ${id} -1
    fi
}

function show_progress {
    #tput rc
    #tput ed

    for ((i=0;i<lines;i++)); do tput cuu1; done
    tput ed

    scale=2
    lines=0
    for i in ${!trace_files[@]}; do
        echo -n ${trace_files[$i]}
        lines=$((lines+1))
        if [ "${result[$i]}" = "success" ]; then
            echo -e " -" $(colored_text "green" "PASS")
        elif [ "${result[$i]}" = "fail" ]; then
            echo -e " -" $(colored_text "red" "FAIL")
        else
            p=$((progress[$i]/scale))
            r=$((100/scale-p))
            completed=$(printf "%${p}s" | tr " " ">")
            remaining=$(printf "%${r}s" | tr " " "-")
            echo 
            echo -e "  [${completed}${remaining}] ${progress[$i]}%"
            lines=$((lines+1))
        fi 
    done
}

while getopts :hs:c:p: flag
do
    case "${flag}" in
        s) SPEC=${OPTARG};;
        c) CONFIG=${OPTARG};;
        p) PARALLEL=${OPTARG};;
        h|*) show_usage; exit 1;; 
    esac
done

shift $(($OPTIND -1))
trace_files=("$@")
progress=()
result=()

if [ ! "$SPEC" ] || [ ! "$CONFIG" ] || [ ! "$trace_files" ] 
then
    show_usage
    exit 1
fi

echo "spec: ${SPEC}"
echo "config: ${CONFIG}"

install_tlaplus

total=${#trace_files[@]}
SECONDS=0

#tput init
#tput sc

export -f validate
while read line; do
    tokens=(${line// / })
    id=${tokens[0]}
    p=${tokens[1]}
    progress[$id]=$p
    if [ "${p}" = 100 ]; then
        result[$id]="success"
        show_progress
    elif [ "${p}" = -1 ]; then
        result[$id]="fail"
    fi

    if [ "${SECONDS}" -ge 2 ]; then
        show_progress
        SECONDS=0
    fi    
done < \
<(for i in ${!trace_files[@]}; do 
    progress[$i]=0
    echo $i ${trace_files[$i]}
done | \
xargs -I{} -P ${PARALLEL} bash -c 'validate $@' _ {} $SPEC $CONFIG $TOOLDIR $STATEDIR)


passed=0
for s in ${result[@]}; do
    if [ "$s" = "success" ]; then 
        passed=$((passed+1))
    fi
done

echo -e "$passed of $total trace(s) passed"

if [ "$passed" -lt "$total" ]; then 
    exit 1
fi


