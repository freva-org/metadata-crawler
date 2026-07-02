#!/bin/sh
# Create/Update mock catalogues
#
this_dir=$(readlink -f $(dirname $(readlink -f $0)))
config_dir=$(readlink -f "$this_dir/../../")
cd $config_dir
for t in fs s3 swift;do
    mdc add $this_dir/${t}-cat.yml --prefix $t -c "$config_dir/drs_config.toml" -ds obs-$t
    sed -i -E 's#^([[:space:]]*urlpath:[[:space:]]*).*/([^/]+)$#\1./\2#' $this_dir/${t}-cat.yml
done
