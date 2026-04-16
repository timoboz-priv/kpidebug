from kpidebug.data.cache.memory import InMemoryTableCache


class TestInMemoryTableCache:
    def test_set_and_get(self):
        cache = InMemoryTableCache()
        rows = [{"id": "1", "amount": 100}]
        cache.set_rows("s1", "charges", rows)
        assert cache.get_rows("s1", "charges") == rows

    def test_get_miss(self):
        cache = InMemoryTableCache()
        assert cache.get_rows("s1", "charges") is None

    def test_is_cached(self):
        cache = InMemoryTableCache()
        assert not cache.is_cached("s1", "charges")
        cache.set_rows("s1", "charges", [])
        assert cache.is_cached("s1", "charges")

    def test_clear_table(self):
        cache = InMemoryTableCache()
        cache.set_rows("s1", "charges", [{"id": "1"}])
        cache.clear_table("s1", "charges")
        assert not cache.is_cached("s1", "charges")
        assert cache.get_rows("s1", "charges") is None

    def test_separate_tables(self):
        cache = InMemoryTableCache()
        cache.set_rows("s1", "charges", [{"id": "c1"}])
        cache.set_rows("s1", "customers", [{"id": "u1"}])
        assert cache.get_rows("s1", "charges") == [{"id": "c1"}]
        assert cache.get_rows("s1", "customers") == [{"id": "u1"}]

    def test_separate_sources(self):
        cache = InMemoryTableCache()
        cache.set_rows("s1", "charges", [{"id": "a"}])
        cache.set_rows("s2", "charges", [{"id": "b"}])
        assert cache.get_rows("s1", "charges") == [{"id": "a"}]
        assert cache.get_rows("s2", "charges") == [{"id": "b"}]

    def test_sync_rows_replaces(self):
        cache = InMemoryTableCache()
        cache.set_rows("s1", "charges", [
            {"id": "1", "amount": 100},
        ])
        cache.sync_rows("s1", "charges", [
            {"id": "1", "amount": 200},
            {"id": "2", "amount": 50},
        ], pk_columns=["id"])

        rows = cache.get_rows("s1", "charges")
        assert len(rows) == 2
        assert rows[0]["amount"] == 200

    def test_overwrite(self):
        cache = InMemoryTableCache()
        cache.set_rows("s1", "charges", [{"id": "old"}])
        cache.set_rows("s1", "charges", [{"id": "new"}])
        assert cache.get_rows("s1", "charges") == [{"id": "new"}]
