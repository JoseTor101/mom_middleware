# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: mom.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'mom.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\tmom.proto\x12\x03mom\"0\n\x0eMessageRequest\x12\r\n\x05topic\x18\x01 \x01(\t\x12\x0f\n\x07message\x18\x02 \x01(\t\"2\n\x0fMessageResponse\x12\x0e\n\x06status\x18\x01 \x01(\t\x12\x0f\n\x07message\x18\x02 \x01(\t2\x87\x01\n\x0eMessageService\x12\x38\n\x0bSendMessage\x12\x13.mom.MessageRequest\x1a\x14.mom.MessageResponse\x12;\n\x0eReceiveMessage\x12\x13.mom.MessageRequest\x1a\x14.mom.MessageResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'mom_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_MESSAGEREQUEST']._serialized_start=18
  _globals['_MESSAGEREQUEST']._serialized_end=66
  _globals['_MESSAGERESPONSE']._serialized_start=68
  _globals['_MESSAGERESPONSE']._serialized_end=118
  _globals['_MESSAGESERVICE']._serialized_start=121
  _globals['_MESSAGESERVICE']._serialized_end=256
# @@protoc_insertion_point(module_scope)
