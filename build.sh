#!/bin/bash

TEST_INSTALL=${1:-test-install=false}

VER=0.0.1
LIBRARY_SCRIPT="library_driver.py"
LIBRARY_SCRIPT_DIR_ARG="foo/bar"
CLI_DIR_ARG="foo/bar"
KEEP_BUILD="true"
# wasn't able to conda init/activate from within script so point to the env binaries directly
# E.g. these don't have the intended effect:
# conda init zsh
# conda activate rkstr8
PY=$(which python)
PIP=$(which pip)

print_banner() {
    local banner_char="#"
    local text="$1"
    local text_length=${#text}
    local print_banner_length=$((text_length + 4))

    printf "%s\n" "$(printf '%*s' "$print_banner_length" | tr ' ' "$banner_char")"
    printf "%s\n" "$banner_char $text $banner_char"
    printf "%s\n" "$(printf '%*s' "$print_banner_length" | tr ' ' "$banner_char")"
}

# run in the base proj directory (the one with .git, src/, etc.)
print_banner "Traversing data dir and generating MANIFEST.in"
find src/rkstr8/data -type f \( ! -iname "*.pyc" \) -exec printf "include %s\n" {} \; > MANIFEST.in
print_banner "Contents of MANIFEST.in"
cat MANIFEST.in

# makes dist/rkstr8-0.0.1-py3-none-any.whl
print_banner "Building the wheel"
$PY -m build --wheel

print_banner "Cleaning up build dirs"
# We may wish to keep the build dir around for debugging purposes
# check if dir exists and flag is set
# bash function to test if build dir exists and flag is set in variable $KEEP_BUILD in single if statment
if [ -d build ] && [ "$KEEP_BUILD" = "false" ]
then
  rm -rf build
fi

if [ -d "rkstr8-${VER}.dist-info" ] && [ "$KEEP_BUILD" = "false" ]
then
  rm -rf "rkstr8-${VER}.dist-info"
fi

print_banner "Listing contents of wheel"
unzip -lo "dist/rkstr8-${VER}-py3-none-any.whl"

# Note the --force-reinstall flag is needed since we aren't incrementing version in our builds
print_banner "Re-install RKSTR8 from just-built wheel"
$PIP install --force-reinstall "dist/rkstr8-${VER}-py3-none-any.whl"

if [ "$TEST_INSTALL" = "test-install=true" ]
then
  # test the wheel using the CLI entry point
  print_banner "Testing the CLI entry point"
  rkstr8 -d $CLI_DIR_ARG

  # test the library entry-point
  print_banner "Testing the library entry point"
  $PY $LIBRARY_SCRIPT -d $LIBRARY_SCRIPT_DIR_ARG
fi
