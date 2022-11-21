#!/bin/bash

# Adapted from https://github.com/learnedsystems/SOSD/blob/master/scripts/download.sh
# and https://gist.github.com/fkraeutli/66fa741d9a8c2a6a238a01d17ed0edc5

function main() {
   mkdir -p SOSD
   mkdir -p domains

   echo "Compiling Binaries..."
   make clean && make all

   echo "Downloading SOSD Datasets..."
   cd SOSD
   download_file_zst books_800M_uint64 8708eb3e1757640ba18dcd3a0dbb53bc https://www.dropbox.com/s/y2u3nbanbnbmg7n/books_800M_uint64.zst?dl=1
   download_file_zst fb_200M_uint64 3b0f820caa0d62150e87ce94ec989978 https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/EATHF7
   
   echo "Downloading Domains Dataset..."
   cd ../domains && download_org_domains
   
   echo "Processing Domains Dataset..."
   process_org_domains   
}

function download_org_domains() {
   for resp in `curl https://api.github.com/repos/tb0hdan/domains/contents/data/generic_org | jq -r '.[] | "\(.sha):\(.name)"'`; do
      sha=$(echo -n $resp | cut -d ":" -f 1)
      name=$(echo -n $resp | cut -d ":" -f 2)
      content=$(curl https://api.github.com/repos/tb0hdan/domains/git/blobs/$sha | jq -r '.content')
      decoded=$(echo -e $content | base64 -di)
      sha256=$(echo "$decoded" | sed -n 2p | cut -d ":" -f 2)
      size=$(echo "$decoded" | sed -n 3p | cut -d " " -f 2)
      json=$(echo '{"operation": "download", "transfer": ["basic"], "objects": [{"oid": "", "size": 0}]}' | jq ".objects[0].oid = \"${sha256}\" | .objects[0].size = ${size}")
      url=$(curl -X POST \
               -H "Accept: application/vnd.git-lfs+json" \
               -H "Content-type: application/json" \
               -d "${json}" \
               https://github.com/tb0hdan/domains.git/info/lfs/objects/batch | jq -r '.objects[0].actions.download.href')
      wget -O $name $url
   done
}

function process_org_domains() {
   xz -d -f *.xz
   cat *.txt > domains.txt
   rm domain2multi-org*.txt
   cat domains.txt | shuf -o domains.txt
}

# Calculate md5 checksum of FILE and stores it in MD5_RESULT
function get_checksum() {
   FILE=$1

   if [ -x "$(command -v md5sum)" ]; then
      # Linux
      MD5_RESULT=`md5sum ${FILE} | awk '{ print $1 }'`
   else
      # OS X
      MD5_RESULT=`md5 -q ${FILE}`
   fi
}

function download_file_zst() {
   FILE=$1;
   CHECKSUM=$2;
   URL=$3;

   # Check if file already exists
   if [ -f ${FILE} ]; then
      # Exists -> check the checksum
      get_checksum ${FILE}
      if [ "${MD5_RESULT}" != "${CHECKSUM}" ]; then
         wget -O - ${URL} | zstd -d > ${FILE}
      fi
   else
      # Does not exists -> download
      wget -O - ${URL} | zstd -d > ${FILE}
   fi

   # Validate (at this point the file should really exist)
   get_checksum ${FILE}
   if [ "${MD5_RESULT}" != "${CHECKSUM}" ]; then
      echo "error checksum does not match: run download again"
      exit -1
   else
      echo ${FILE} "checksum ok"
   fi
}

# Run
main