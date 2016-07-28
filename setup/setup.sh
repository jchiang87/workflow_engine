inst_dir=$( cd $(dirname $BASH_SOURCE)/..; pwd -P )
export PYTHONPATH=$inst_dir/python:${PYTHONPATH}
export WORKFLOW_ENGINE_DIR=$inst_dir
