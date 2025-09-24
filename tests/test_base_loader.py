import pytest
from py_load_medgen.loader.base import AbstractNativeLoader

# A concrete implementation of the abstract base class for testing purposes
class ConcreteLoader(AbstractNativeLoader):
    def connect(self):
        pass
    def close(self):
        pass
    def initialize_staging(self, table_name, ddl):
        super().initialize_staging(table_name, ddl)
    def bulk_load(self, table_name, data_iterator):
        super().bulk_load(table_name, data_iterator)
    def execute_cdc(self, staging_table, production_table, pk_name, business_key):
        return super().execute_cdc(staging_table, production_table, pk_name, business_key)
    def apply_changes(self, mode, staging_table, production_table, production_ddl, index_ddls, pk_name, business_key):
        super().apply_changes(mode, staging_table, production_table, production_ddl, index_ddls, pk_name, business_key)
    def cleanup(self, staging_table, production_table):
        super().cleanup(staging_table, production_table)
    def log_run_start(self, run_id, package_version, load_mode, source_files):
        return super().log_run_start(run_id, package_version, load_mode, source_files)
    def log_run_finish(self, log_id, status, records_extracted, records_loaded, error_message=None):
        super().log_run_finish(log_id, status, records_extracted, records_loaded, error_message)

@pytest.mark.unit
def test_abstract_native_loader_raises_not_implemented():
    """
    Tests that the abstract methods of AbstractNativeLoader raise
    NotImplementedError.
    """
    loader = ConcreteLoader()

    with pytest.raises(NotImplementedError):
        loader.initialize_staging("test", "CREATE TABLE test (id INT);")

    with pytest.raises(NotImplementedError):
        loader.bulk_load("test", iter([]))

    with pytest.raises(NotImplementedError):
        loader.execute_cdc("staging", "production", "id", "id")

    with pytest.raises(NotImplementedError):
        loader.apply_changes("full", "staging", "production", "ddl", [], "id", "id")

    with pytest.raises(NotImplementedError):
        loader.cleanup("staging", "production")

    with pytest.raises(NotImplementedError):
        loader.log_run_start(None, "1.0", "full", {})

    with pytest.raises(NotImplementedError):
        loader.log_run_finish(1, "success", 1, 1)

@pytest.mark.unit
def test_context_manager():
    """
    Tests that the context manager calls connect and close.
    """
    class ConnectCloseTracker(AbstractNativeLoader):
        def __init__(self):
            self.connect_called = False
            self.close_called = False
        def connect(self):
            self.connect_called = True
        def close(self):
            self.close_called = True
        def initialize_staging(self, table_name, ddl): pass
        def bulk_load(self, table_name, data_iterator): pass
        def execute_cdc(self, staging_table, production_table, pk_name, business_key): pass
        def apply_changes(self, mode, staging_table, production_table, production_ddl, index_ddls, pk_name, business_key): pass
        def cleanup(self, staging_table, production_table): pass
        def log_run_start(self, run_id, package_version, load_mode, source_files): pass
        def log_run_finish(self, log_id, status, records_extracted, records_loaded, error_message=None): pass

    tracker = ConnectCloseTracker()
    assert not tracker.connect_called
    assert not tracker.close_called

    with tracker:
        assert tracker.connect_called
        assert not tracker.close_called

    assert tracker.close_called
