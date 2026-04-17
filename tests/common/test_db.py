import os
from unittest.mock import patch, MagicMock

import pytest

from kpidebug.common.db import ConnectionPoolManager


class TestConnectionPoolManager:
    def setup_method(self):
        ConnectionPoolManager._instance = None

    def test_raises_without_database_url(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DATABASE_URL", None)
            with pytest.raises(RuntimeError, match="DATABASE_URL"):
                ConnectionPoolManager()

    @patch("kpidebug.common.db.ConnectionPool")
    def test_creates_pool_with_database_url(self, mock_pool_cls):
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool

        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://test:test@localhost/test"}):
            manager = ConnectionPoolManager()

        assert manager.pool() is mock_pool
        mock_pool_cls.assert_called_once_with(
            conninfo="postgresql://test:test@localhost/test",
            min_size=2,
            max_size=10,
        )

    @patch("kpidebug.common.db.ConnectionPool")
    def test_get_instance_returns_singleton(self, mock_pool_cls):
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://test:test@localhost/test"}):
            a = ConnectionPoolManager.get_instance()
            b = ConnectionPoolManager.get_instance()
        assert a is b

    @patch("kpidebug.common.db.ConnectionPool")
    def test_close(self, mock_pool_cls):
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool

        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://test:test@localhost/test"}):
            manager = ConnectionPoolManager()
        manager.close()
        mock_pool.close.assert_called_once()
        assert manager._pool is None
