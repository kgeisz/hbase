# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# conftest.py is a special pytest file. Fixtures and hooks defined here are
# automatically available to all tests in this directory and its subdirectories.

import logging
import os

import pytest

from python.src.logger_config import LOG_FORMAT

DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'output')


@pytest.fixture(autouse=True)
def per_test_log_file(request):
    output_dir = os.environ.get('OUTPUT_DIR', DEFAULT_OUTPUT_DIR)
    test_name = request.node.name
    log_dir = os.path.join(output_dir, test_name)
    os.makedirs(log_dir, exist_ok=True)

    execution_count = getattr(request.node, 'execution_count', 1)
    log_path = os.path.join(log_dir, f"{test_name}.run{execution_count}.log")

    handler = logging.FileHandler(log_path, mode='w')
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.setLevel(logging.DEBUG)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    yield

    root_logger.removeHandler(handler)
    handler.close()
