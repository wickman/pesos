# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: mesos/module/module.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from ... import mesos


DESCRIPTOR = _descriptor.FileDescriptor(
  name='mesos/module/module.proto',
  package='mesos',
  serialized_pb=_b('\n\x19mesos/module/module.proto\x12\x05mesos\x1a\x11mesos/mesos.proto\"\xca\x01\n\x07Modules\x12)\n\tlibraries\x18\x01 \x03(\x0b\x32\x16.mesos.Modules.Library\x1a\x93\x01\n\x07Library\x12\x0c\n\x04\x66ile\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12.\n\x07modules\x18\x03 \x03(\x0b\x32\x1d.mesos.Modules.Library.Module\x1a<\n\x06Module\x12\x0c\n\x04name\x18\x01 \x01(\t\x12$\n\nparameters\x18\x02 \x03(\x0b\x32\x10.mesos.ParameterB\x1a\n\x10org.apache.mesosB\x06Protos')
  ,
  dependencies=[mesos.mesos_pb2.DESCRIPTOR,])
_sym_db.RegisterFileDescriptor(DESCRIPTOR)




_MODULES_LIBRARY_MODULE = _descriptor.Descriptor(
  name='Module',
  full_name='mesos.Modules.Library.Module',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='mesos.Modules.Library.Module.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='parameters', full_name='mesos.Modules.Library.Module.parameters', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=198,
  serialized_end=258,
)

_MODULES_LIBRARY = _descriptor.Descriptor(
  name='Library',
  full_name='mesos.Modules.Library',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='file', full_name='mesos.Modules.Library.file', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='name', full_name='mesos.Modules.Library.name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='modules', full_name='mesos.Modules.Library.modules', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_MODULES_LIBRARY_MODULE, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=111,
  serialized_end=258,
)

_MODULES = _descriptor.Descriptor(
  name='Modules',
  full_name='mesos.Modules',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='libraries', full_name='mesos.Modules.libraries', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_MODULES_LIBRARY, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=56,
  serialized_end=258,
)

_MODULES_LIBRARY_MODULE.fields_by_name['parameters'].message_type = mesos.mesos_pb2._PARAMETER
_MODULES_LIBRARY_MODULE.containing_type = _MODULES_LIBRARY
_MODULES_LIBRARY.fields_by_name['modules'].message_type = _MODULES_LIBRARY_MODULE
_MODULES_LIBRARY.containing_type = _MODULES
_MODULES.fields_by_name['libraries'].message_type = _MODULES_LIBRARY
DESCRIPTOR.message_types_by_name['Modules'] = _MODULES

Modules = _reflection.GeneratedProtocolMessageType('Modules', (_message.Message,), dict(

  Library = _reflection.GeneratedProtocolMessageType('Library', (_message.Message,), dict(

    Module = _reflection.GeneratedProtocolMessageType('Module', (_message.Message,), dict(
      DESCRIPTOR = _MODULES_LIBRARY_MODULE,
      __module__ = 'mesos.module.module_pb2'
      # @@protoc_insertion_point(class_scope:mesos.Modules.Library.Module)
      ))
    ,
    DESCRIPTOR = _MODULES_LIBRARY,
    __module__ = 'mesos.module.module_pb2'
    # @@protoc_insertion_point(class_scope:mesos.Modules.Library)
    ))
  ,
  DESCRIPTOR = _MODULES,
  __module__ = 'mesos.module.module_pb2'
  # @@protoc_insertion_point(class_scope:mesos.Modules)
  ))
_sym_db.RegisterMessage(Modules)
_sym_db.RegisterMessage(Modules.Library)
_sym_db.RegisterMessage(Modules.Library.Module)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), _b('\n\020org.apache.mesosB\006Protos'))
# @@protoc_insertion_point(module_scope)