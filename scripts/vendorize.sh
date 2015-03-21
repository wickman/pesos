# Script to clone the mesos repo and generate vendorized internal protobufs.

CLONE_TEMP=$(mktemp -d -t mesos)
MESSAGE_TEMP=$(mktemp -d -t mesos_protos)
PB_TEMP=$(mktemp -d -t mesos_pb2)

git clone https://github.com/apache/mesos $CLONE_TEMP/mesos

# stage
cp -r $CLONE_TEMP/mesos/include $MESSAGE_TEMP
mkdir $MESSAGE_TEMP/include/mesos/internal
cp $CLONE_TEMP/mesos/src/master/*.proto $MESSAGE_TEMP/include/mesos/internal
cp $CLONE_TEMP/mesos/src/messages/*.proto $MESSAGE_TEMP/include/mesos/internal

# compile
protoc -I$MESSAGE_TEMP/include $(find $MESSAGE_TEMP/include -iname \*.proto) --python_out=$PB_TEMP

# cleanup
rm -rf $CLONE_TEMP
rm -rf $MESSAGE_TEMP

# postprocess
find $PB_TEMP -iname \*_pb2.py | xargs sed -i~ 's/import mesos.mesos_pb2/from ... import mesos/'
for dir in $(find $PB_TEMP/mesos -type d); do
  touch $dir/__init__.py
done
echo 'from . import mesos_pb2' > $PB_TEMP/mesos/__init__.py
echo "Vendored into $PB_TEMP"
