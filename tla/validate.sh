#!/usr/bin/env bash

WORKDIR="$(mktemp -d)"
TOOLDIR="${WORKDIR}/tool"
STATEDIR="${WORKDIR}/state"
WORKDIR="$(mktemp -d)"
TOOLDIR="${WORKDIR}/tool"
STATEDIR="${WORKDIR}/state"
FAILFAST=false
PARALLEL=$(nproc)

function show_usage {
    echo "usage: validate.sh [-p <parallel>] -s <spec> -c <config> <trace files>">&2
}

function install_tlaplus {
    echo -n "Downloading TLA+ tools ... "
    wget -qN https://nightly.tlapl.us/dist/tla2tools.jar -P ${TOOLDIR}
    wget -qN https://github.com/tlaplus/CommunityModules/releases/latest/download/CommunityModules-deps.jar -P ${TOOLDIR}
    echo "done."
}
PARALLEL=$(nproc)

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
    local id=${1}
    local trace=${2}
    local spec=${3}
    local config=${4}
    local tooldir=${5}
    local statedir=${6}
    local name=$(basename $trace .ndjson)

    preprocess_log $trace

    set -o pipefail
    env JSON="${trace}" java -XX:+UseParallelGC -cp ${tooldir}/tla2tools.jar:${tooldir}/CommunityModules-deps.jar tlc2.TLC -config "${config}" "${spec}" -lncheck final -metadir "${statedir}" -fpmem 0.9  | sed -nuE "s/<<\"Progress %:\", ([0-9]+)>>$/${id} \1/p"

    if [ "$?" -ne 0 ]; then
        echo ${id} -1
    fi
}

function show_progress {
    p=0
    r=100
    total=${#trace_files[@]}
    pending=0
    running=0
    for i in ${!trace_files[@]}; do
        if [ "${result[$i]}" = "success" ] && [ "${printed[$i]}" = "" ]; then
            p=$((p+100))
            echo -e ${trace_files[$i]} "-" $(colored_text "green" "PASS") "\033[0K"
            printed[$i]="true"
        elif [ "${result[$i]}" = "fail" ] && [ "${printed[$i]}" = "" ]; then
            p=$((p+100))
            echo -e ${trace_files[$i]} "-" $(colored_text "red" "FAIL") "\033[0K"
            printed[$i]="true"
        elif [ "${result[$i]}" = "" ]; then
            p=$((p+progress[$i]))
            if [ $((progress[$i])) -gt 0 ]; then
                running=$((running+1))
            else 
                pending=$((pending+1))
            fi
        else 
            p=$((p+100))
        fi 
    done
    if [ $((running+pending)) -gt 0 ]; then 
        bar_len=$(($(tput cols) - 45))
        p=$((p/total))
        p_bar=$((p*bar_len/100))
        r_bar=$((bar_len-p_bar))
        completed=$(printf "%${p_bar}s" | tr " " "#")
        remaining=$(printf "%${r_bar}s" | tr " " ".")
        
        echo -ne "  [${completed}${remaining}] running: ${running} pending: $((pending)) progress: ${p}%\r"
    fi
}

while getopts :hs:c:p: flag
do
    case "${flag}" in
        s) SPEC=${OPTARG};;
        c) CONFIG=${OPTARG};;
        p) PARALLEL=${OPTARG};;
        h|*) show_usage; exit 1;; 
        p) PARALLEL=${OPTARG};;
        h|*) show_usage; exit 1;; 
    esac
done

shift $(($OPTIND -1))
trace_files=("$@")
printed=()
progress=()
result=()

if [ ! "$SPEC" ] || [ ! "$CONFIG" ] || [ ! "$trace_files" ] 
then
    show_usage
    exit 1
fi

echo "spec: ${SPEC}"
echo "config: ${CONFIG}"
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

export -f validate
export -f preprocess_log
while read line; do
    tokens=(${line// / })
    id=${tokens[0]}
    p=${tokens[1]}
    progress[$id]=$p
    if [ "${p}" = 100 ] && [ "${result[$id]}" = "" ]; then
        result[$id]="success"
        show_progress
    elif [ "${p}" = -1 ] && [ "${result[$id]}" = "" ]; then
        result[$id]="fail"
        show_progress
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

if [ $passed -lt $total ]; then 
    exit 1
fi
