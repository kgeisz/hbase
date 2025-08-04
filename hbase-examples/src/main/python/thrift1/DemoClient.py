#!/usr/bin/env python
'''
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
'''

import sys
import time
import os
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from gen_py.hbase import ttypes
from gen_py.hbase.Hbase import Client, ColumnDescriptor, Mutation

def printVersions(row, versions):
  print("row: " + row.decode() + ", values: ", end=' ')
  for cell in versions:
    print(cell.value.decode() + "; ", end=' ')
  print()

def printRow(entry):
  print("row: " + entry.row.decode() + ", cols:", end=' ')
  for k in sorted(entry.columns):
    print(k.decode() + " => " + entry.columns[k].value.decode(), end=' ')
  print()


def demo_client(host, port, is_framed_transport):

  # Make socket
  socket = TSocket.TSocket(host, port)

  # Make transport
  if is_framed_transport:
    transport = TTransport.TFramedTransport(socket)
  else:
    transport = TTransport.TBufferedTransport(socket)

  # Wrap in a protocol
  protocol = TBinaryProtocol.TBinaryProtocol(transport)

  # Create a client to use the protocol encoder
  client = Client(protocol)

  # Connect!
  transport.open()

  # Check Thrift Server Type
  serverType = client.getThriftServerType()
  if serverType != ttypes.TThriftServerType.ONE:
    raise RuntimeError(f"Mismatch between client and server, server type is {serverType}")

  demo_table = b"demo_table"

  #
  # Scan all tables, look for the demo table and delete it.
  #
  print("scanning tables...")
  for table in client.getTableNames():
    print(f"  found: {table}")
    if table == demo_table:
      if client.isTableEnabled(table):
        print(f"    disabling table: {demo_table}")
        client.disableTable(table)
      print(f"    deleting table: {demo_table}")
      client.deleteTable(table)

  columns = []
  col = ColumnDescriptor()
  col.name = b'entry:'
  col.maxVersions = 10
  columns.append(col)
  col = ColumnDescriptor()
  col.name = b'unused:'
  columns.append(col)

  try:
    print(f"creating table: {demo_table}")
    client.createTable(demo_table, columns)
  except ttypes.AlreadyExists as ae:
    print("WARN: " + ae.message)

  cols = client.getColumnDescriptors(demo_table)
  print(f"column families in {demo_table}")
  for col_name in cols.keys():
    col = cols[col_name]
    print(f"  column: {col.name}, maxVer: {col.maxVersions}")

  dummy_attributes = {}
  #
  # Test UTF-8 handling
  #
  non_utf8 = bytes("foo-\xfc\xa1\xa1\xa1\xa1\xa1", 'cp037')  # IBM037, IBM039 encoding
  valid_utf8 = bytes("foo-\xE7\x94\x9F\xE3\x83\x93\xE3\x83\xBC\xE3\x83\xAB", 'utf-8')

  # non-utf8 is fine for data
  mutations = [Mutation(column=b"entry:foo",value=non_utf8)]
  print(str(mutations))
  client.mutateRow(demo_table, b"foo", mutations, dummy_attributes)

  # try empty strings
  try:
    mutations = [Mutation(column=b"entry:", value=b"")]
    client.mutateRow(demo_table, b"", mutations, dummy_attributes)
  except ttypes.IllegalArgument as e:
      print(f'expected exception: {e.message}')

  # this row name is valid utf8
  mutations = [Mutation(column=b"entry:foo", value=valid_utf8)]
  client.mutateRow(demo_table, valid_utf8, mutations, dummy_attributes)

  # non-utf8 is not allowed in row names
  try:
    mutations = [Mutation(column=b"entry:foo", value=non_utf8)]
    client.mutateRow(demo_table, non_utf8, mutations, dummy_attributes)
  except ttypes.IOError as e:
    print(f'expected exception: {e.message}')

  # Run a scanner on the rows we just created
  print("Starting scanner...")
  scanner = client.scannerOpen(demo_table, b"", [b"entry:"], dummy_attributes)

  r = client.scannerGet(scanner)
  while r:
    printRow(r[0])
    r = client.scannerGet(scanner)
  print("Scanner finished")

  #
  # Run some operations on a bunch of rows.
  #
  for e in range(100, 0, -1):
    # format row keys as "00000" to "00100"
    row = bytes(f"{e:05}", 'utf-8')
    print(f"kevin: row = {row}")

    mutations = [Mutation(column=b"unused:", value=b"DELETE_ME")]
    client.mutateRow(demo_table, row, mutations, dummy_attributes)
    printRow(client.getRow(demo_table, row, dummy_attributes)[0])
    client.deleteAllRow(demo_table, row, dummy_attributes)

    mutations = [Mutation(column=b"entry:num", value=b"0"),
                 Mutation(column=b"entry:foo", value=b"FOO")]
    print(f"kevin: {str(mutations)}")
    client.mutateRow(demo_table, row, mutations, dummy_attributes)
    # TODO - client.getRow() is throwing an IndexError
    print(f"kevin: (row: {row}): trying to get row")
    print(f"kevin: t = {demo_table}")
    print(f"kevin: row = {row}")
    print(f"kevin: dummy_attributes = {dummy_attributes}")
    time.sleep(1)
    # retrieved_row = None
    # for i in range(1, 6):
    #     print(f"kevin: Attempt {i} for getRow()")
    #     retrieved_row = client.getRow(t, row, dummy_attributes)
    #     if retrieved_row:
    #         break
    #     time.sleep(0.2)
    retrieved_row = client.getRow(demo_table, row, dummy_attributes)
    print(f"kevin: retrieved_row: {retrieved_row}")
    printRow(retrieved_row[0])

    mutations = [Mutation(column=b"entry:foo",isDelete=True),
                 Mutation(column=b"entry:num",value=b"-1")]
    client.mutateRow(demo_table, row, mutations, dummy_attributes)
    printRow(client.getRow(demo_table, row, dummy_attributes)[0])

    mutations = [Mutation(column=b"entry:num", value=bytes(str(e), 'utf-8')),
                 Mutation(column=b"entry:sqr", value=bytes(str(e*e), 'utf-8'))]
    client.mutateRow(demo_table, row, mutations, dummy_attributes)
    printRow(client.getRow(demo_table, row, dummy_attributes)[0])

    # time.sleep(0.05)

    mutations = [Mutation(column=b"entry:num",value=b"-999"),
                 Mutation(column=b"entry:sqr",isDelete=True)]
    client.mutateRowTs(demo_table, row, mutations, 1, dummy_attributes) # shouldn't override latest
    printRow(client.getRow(demo_table, row, dummy_attributes)[0])

    versions = client.getVer(demo_table, row, b"entry:num", 10, dummy_attributes)
    printVersions(row, versions)
    if len(versions) != 3:
      print("FATAL: wrong # of versions")
      sys.exit(-1)

    r = client.get(demo_table, row, b"entry:foo", dummy_attributes)
    # just to be explicit, we get lists back, if it's empty there was no matching row.
    if len(r) > 0:
      raise RuntimeError("shouldn't get here!")

    print("----------------------------------")

  columnNames = []
  for (col, desc) in client.getColumnDescriptors(demo_table).items():
    desc_name = desc.name.decode()
    print(f"column with name: {desc_name}")
    print(desc)
    columnNames.append(bytes(f"{desc_name}:", 'utf-8'))

  print("Starting scanner...")
  scanner = client.scannerOpenWithStop(demo_table, b"00020", b"00040", columnNames, dummy_attributes)

  r = client.scannerGet(scanner)
  while r:
    printRow(r[0])
    r = client.scannerGet(scanner)

  client.scannerClose(scanner)
  print("Scanner finished")

  transport.close()


if __name__ == '__main__':
  if len(sys.argv) < 3:
    print(f'usage: {__file__} <host> <port>')
    sys.exit(1)

  host = sys.argv[1]
  port = sys.argv[2]
  is_framed_transport = False
  demo_client(host, port, is_framed_transport)
