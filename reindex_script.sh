#!/bin/bash
echo "Running reindex script"
search_dir="test"
origin_dir="origin_bag"

if [[ ! -d $origin_dir ]]
then
    mkdir $origin_dir
fi

for f in "$search_dir"/*
do
    prefix="${f%.*}"
    name="${prefix##*/}"
    echo "Reindex for $f"
    rosbag reindex $f
    mv "${prefix}.orig.bag" "${origin_dir}/${name}.orig.bag"
done
