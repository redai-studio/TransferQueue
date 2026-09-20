# Copyright 2025 Huawei Technologies Co., Ltd. All Rights Reserved.
# Copyright 2025 The TransferQueue Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from types import SimpleNamespace

import pytest

from transfer_queue.storage.managers.base import StorageManager
from transfer_queue.utils.zmq_utils import ZMQRequestType


class _FakeNotifySocket:
    def __init__(self, connect_error: Exception | None = None) -> None:
        self.closed = False
        self.connect_error = connect_error

    def setsockopt(self, *args, **kwargs) -> None:
        pass

    def connect(self, *args, **kwargs) -> None:
        if self.connect_error is not None:
            raise self.connect_error

    async def send_multipart(self, request) -> None:
        pass

    async def recv_multipart(self, copy=False):
        return [b"ack"]

    def close(self, linger=0) -> None:
        self.closed = True


def _manager(controller_info=None):
    if controller_info is None:
        controller_info = SimpleNamespace(
            id="controller",
            ip="127.0.0.1",
            to_addr=lambda name: "inproc://controller",
        )
    return SimpleNamespace(
        storage_manager_id="notification-test",
        zmq_context=object(),
        controller_info=controller_info,
    )


def _ack(success: bool, partition_id: str = "p0"):
    return SimpleNamespace(
        request_type=ZMQRequestType.NOTIFY_DATA_UPDATE_ACK,
        sender_id="controller",
        body={"success": success, "partition_id": partition_id},
    )


@pytest.mark.asyncio
async def test_notify_data_update_rejects_missing_controller():
    manager = _manager(controller_info=False)

    with pytest.raises(RuntimeError, match="has no controller"):
        await StorageManager.notify_data_update(manager, "p0", [], {}, {})


@pytest.mark.asyncio
async def test_notify_and_wait_requires_positive_ack(monkeypatch):
    socket = _FakeNotifySocket()
    monkeypatch.setattr("transfer_queue.storage.managers.base.create_zmq_socket", lambda **kwargs: socket)
    monkeypatch.setattr("transfer_queue.storage.managers.base.ZMQMessage.deserialize", lambda messages: _ack(False))

    with pytest.raises(RuntimeError, match="rejected the production-status update"):
        await StorageManager._notify_and_wait(_manager(), [b"request"])
    assert socket.closed is True


@pytest.mark.asyncio
async def test_notify_and_wait_accepts_positive_ack(monkeypatch):
    socket = _FakeNotifySocket()
    monkeypatch.setattr("transfer_queue.storage.managers.base.create_zmq_socket", lambda **kwargs: socket)
    monkeypatch.setattr("transfer_queue.storage.managers.base.ZMQMessage.deserialize", lambda messages: _ack(True))

    await StorageManager._notify_and_wait(_manager(), [b"request"])
    assert socket.closed is True


@pytest.mark.asyncio
async def test_notify_and_wait_times_out_without_ack(monkeypatch):
    socket = _FakeNotifySocket()
    monkeypatch.setattr("transfer_queue.storage.managers.base.create_zmq_socket", lambda **kwargs: socket)
    monkeypatch.setattr("transfer_queue.storage.managers.base.TQ_DATA_UPDATE_RESPONSE_TIMEOUT", 0)

    with pytest.raises(TimeoutError, match="production-status ACK"):
        await StorageManager._notify_and_wait(_manager(), [b"request"])
    assert socket.closed is True


@pytest.mark.asyncio
async def test_notify_and_wait_closes_socket_when_connect_fails(monkeypatch):
    socket = _FakeNotifySocket(connect_error=ConnectionError("controller unavailable"))
    monkeypatch.setattr("transfer_queue.storage.managers.base.create_zmq_socket", lambda **kwargs: socket)

    with pytest.raises(ConnectionError, match="controller unavailable"):
        await StorageManager._notify_and_wait(_manager(), [b"request"])
    assert socket.closed is True
