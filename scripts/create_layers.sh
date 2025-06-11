#!/usr/bin/env bash
set -Eeo pipefail

setup_git() {
    git config --global user.email "no-reply@build.com"
    git config --global user.name "CircleCI"
    git checkout master
    # Avoid version failure
    git stash
    git pull origin master
}

echo ".____                  .__                  .__        ";
echo "|    |    __ __  _____ |__| ____   ____     |__| ____  ";
echo "|    |   |  |  \/     \|  |/ ___\ /  _ \    |  |/  _ \ ";
echo "|    |___|  |  /  Y Y  \  / /_/  >  <_> )   |  (  <_> )";
echo "|_______ \____/|__|_|  /__\___  / \____/ /\ |__|\____/ ";
echo "        \/           \/  /_____/         \/            ";
echo
echo "Update Python Tracer Layers"

setup_git

cd .. && git clone git@github.com:lumigo-io/larn.git
cd larn && npm i -g
larn -r python3.6 -n layers/LAYERS36 --filter lumigo-python-tracer -p ~/python_tracer
larn -r python3.7 -n layers/LAYERS37 --filter lumigo-python-tracer -p ~/python_tracer
cd ../python_tracer
git add layers/LAYERS36.md
git add layers/LAYERS37.md
git commit -m "layers-table: layers md [skip ci]"
git push origin master

echo "Done"
