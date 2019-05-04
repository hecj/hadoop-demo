#!/bin/sh
rm -rf target
git status
git add .
echo '提交描述：'$1
git commit -m $1
git push origin master

