#!/bin/bash

# Time marker for both stderr and stdout
date 1>&2

DEFAULT_PRESTO_HOME=/var/lib/presto
NODE_PROPERTIES_PATH=$DEFAULT_PRESTO_HOME/node.properties
JVM_DUMMY_CONFIG_PATH=$CONF_DIR/jvm.dummy.config
JVM_CONFIG_PATH=$CONF_DIR/etc/jvm.config
export JAVA_HOME=$CDH_PRESTO_JAVA_HOME
HIVE_CONF_PATH=$CONF_DIR/hive-conf

CMD=$1

function log {
  timestamp=$(date)
  echo "$timestamp: $1"	   #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

function generate_jvm_config {
  if [ -f $JVM_DUMMY_CONFIG_PATH ]; then
    cat $JVM_DUMMY_CONFIG_PATH | perl -e '$line = <STDIN>; chomp $line; $configs = substr($line, (length "jvm.config=")); for $value (split /\\n/, $configs) { print $value . "\n" }' > $JVM_CONFIG_PATH
  fi
}

function copy_hdfs_config {
  cp $HIVE_CONF_PATH/core-site.xml $CONF_DIR/etc/catalog
  cp $HIVE_CONF_PATH/hdfs-site.xml $CONF_DIR/etc/catalog
}

function link_files {
  # PRESTO_BIN=$CONF_DIR/bin
  # if [ -L $PRESTO_BIN ]; then
  #   rm -rf $PRESTO_BIN
  # fi
  # ln -s $CDH_PRESTO_HOME/bin $PRESTO_BIN
  cp -r $CDH_PRESTO_HOME/bin $CONF_DIR

  PRESTO_LIB=$CONF_DIR/lib
  if [ -L $PRESTO_LIB ]; then
    rm -rf $PRESTO_LIB
  fi
  ln -s $CDH_PRESTO_HOME/lib $PRESTO_LIB

  PRESTO_PLUGIN=$CONF_DIR/plugin
  if [ -L $PRESTO_PLUGIN ]; then
    rm -rf $PRESTO_PLUGIN
  fi
  ln -s $CDH_PRESTO_HOME/plugin $PRESTO_PLUGIN

  PRESTO_NODE_PROPERTIES=$CONF_DIR/etc/node.properties
  if [ -L $PRESTO_NODE_PROPERTIES ]; then
      rm $PRESTO_NODE_PROPERTIES
  fi
  ln -s $NODE_PROPERTIES_PATH $PRESTO_NODE_PROPERTIES
}

ARGS=()

case $CMD in

  (start_corrdinator)
    log "Startitng Presto Coordinator"
    link_files
    generate_jvm_config
    copy_hdfs_config
    ARGS=("--config")
    ARGS+=("$CONF_DIR/$2")
    ARGS+=("--data-dir")
    ARGS+=("$DEFAULT_PRESTO_HOME")
    ARGS+=("run")
    ;;

  (start_discovery)
    log "Startitng Presto Discovery"
    link_files
    generate_jvm_config
    copy_hdfs_config
    ARGS=("--config")
    ARGS+=("$CONF_DIR/$2")
    ARGS+=("--data-dir")
    ARGS+=("$DEFAULT_PRESTO_HOME")
    ARGS+=("run")
    ;;

  (start_worker)
    log "Startitng Presto Worker"
    link_files
    generate_jvm_config
    copy_hdfs_config
    ARGS=("--config")
    ARGS+=("$CONF_DIR/$2")
    ARGS+=("--data-dir")
    ARGS+=("$DEFAULT_PRESTO_HOME")
    ARGS+=("run")
    ;;

  (init_node_properties)
    if [ ! -f "$NODE_PROPERTIES_PATH" ]; then
      echo "node.environment=production" > $NODE_PROPERTIES_PATH
      echo "node.data-dir=/var/lib/presto" >> $NODE_PROPERTIES_PATH
      echo "node.id=`uuidgen`" >> $NODE_PROPERTIES_PATH
      log "create $NODE_PROPERTIES_PATH successfly"
    else
      log "$NODE_PROPERTIES_PATH is already created"
    fi
    exit 0

    ;;

  (*)
    log "Don't understand [$CMD]"
    ;;

esac

export PATH=$CDH_PRESTO_JAVA_HOME/bin:$PATH
cmd="$CONF_DIR/bin/launcher ${ARGS[@]}"
echo "Run [$cmd]"
exec $cmd
