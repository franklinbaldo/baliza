"""
Dependency Injection Container for BALIZA CLI

Addresses the architectural issue of tight coupling in CLI commands
by providing configurable dependency injection for better testability
and modularity.
"""

from typing import Any, Protocol

from baliza.config import settings


class ExtractorProtocol(Protocol):
    """Protocol for extractor dependencies."""
    
    async def __aenter__(self):
        """Async context manager entry."""
        ...
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        ...
    
    async def extract_data(self, start_date, end_date, force: bool = False):
        """Extract data using legacy method."""
        ...
    
    async def extract_dbt_driven(self, start_date, end_date, use_existing_plan: bool = True):
        """Extract data using dbt-driven method."""
        ...


class TransformerProtocol(Protocol):
    """Protocol for transformer dependencies."""
    
    def transform(self) -> None:
        """Transform data using dbt."""
        ...


class LoaderProtocol(Protocol):
    """Protocol for loader dependencies."""
    
    def load(self, schemas: list[str] | None = None, max_retries: int = 3) -> dict[str, Any]:
        """Load data to Internet Archive."""
        ...


class DependencyContainer:
    """Dependency injection container for CLI commands."""
    
    def __init__(self):
        self._extractors = {}
        self._transformers = {}
        self._loaders = {}
        self._configs = {}
    
    def register_extractor(self, name: str, factory: callable):
        """Register an extractor factory."""
        self._extractors[name] = factory
    
    def register_transformer(self, name: str, factory: callable):
        """Register a transformer factory."""
        self._transformers[name] = factory
    
    def register_loader(self, name: str, factory: callable):
        """Register a loader factory."""
        self._loaders[name] = factory
    
    def register_config(self, name: str, config: Any):
        """Register a configuration object."""
        self._configs[name] = config
    
    def get_extractor(self, name: str = "default", **kwargs) -> ExtractorProtocol:
        """Get an extractor instance."""
        if name not in self._extractors:
            raise ValueError(f"Unknown extractor: {name}")
        return self._extractors[name](**kwargs)
    
    def get_transformer(self, name: str = "default") -> TransformerProtocol:
        """Get a transformer instance."""
        if name not in self._transformers:
            raise ValueError(f"Unknown transformer: {name}")
        return self._transformers[name]()
    
    def get_loader(self, name: str = "default") -> LoaderProtocol:
        """Get a loader instance."""
        if name not in self._loaders:
            raise ValueError(f"Unknown loader: {name}")
        return self._loaders[name]()
    
    def get_config(self, name: str) -> Any:
        """Get a configuration object."""
        if name not in self._configs:
            raise ValueError(f"Unknown config: {name}")
        return self._configs[name]


class CLIServices:
    """Service layer for CLI commands with dependency injection."""
    
    def __init__(self, container: DependencyContainer):
        self.container = container
    
    async def run_extraction(self, start_date, end_date, concurrency: int, force_db: bool, 
                           force: bool, use_dbt: bool = True):
        """Run extraction with injected dependencies."""
        extractor_kwargs = {
            'concurrency': concurrency,
            'force_db': force_db
        }
        
        async with self.container.get_extractor("default", **extractor_kwargs) as extractor:
            if use_dbt:
                return await extractor.extract_dbt_driven(start_date, end_date, not force)
            else:
                return await extractor.extract_data(start_date, end_date, force)
    
    def run_transformation(self):
        """Run transformation with injected dependencies."""
        transformer = self.container.get_transformer("default")
        return transformer.transform()
    
    def run_load(self, schemas: list[str] | None = None, max_retries: int = 3):
        """Run load with injected dependencies."""
        loader = self.container.get_loader("default")
        return loader.load(schemas, max_retries)


def create_default_container() -> DependencyContainer:
    """Create the default dependency container with production dependencies."""
    container = DependencyContainer()
    
    # Register production implementations
    def create_async_extractor(concurrency: int = 10, force_db: bool = False):
        from baliza.extractor import AsyncPNCPExtractor
        return AsyncPNCPExtractor(concurrency=concurrency, force_db=force_db)
    
    def create_dbt_transformer():
        from baliza import transformer
        return transformer
    
    def create_ia_loader():
        from baliza import loader
        return loader
    
    container.register_extractor("default", create_async_extractor)
    container.register_transformer("default", create_dbt_transformer)
    container.register_loader("default", create_ia_loader)
    
    # Register configurations
    container.register_config("settings", settings)
    
    return container


def create_test_container() -> DependencyContainer:
    """Create a test dependency container with mock implementations."""
    container = DependencyContainer()
    
    # Test implementations would go here
    # This allows for easy testing of CLI commands
    
    class MockExtractor:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            # Mock async context manager exit
            pass
        
        async def extract_data(self, start_date, end_date, force=False):
            # Mock data extraction
            return {"total_records_extracted": 1000, "run_id": "test"}
        
        async def extract_dbt_driven(self, start_date, end_date, use_existing_plan=True):
            # Mock dbt-driven extraction
            return {"total_tasks": 10, "completed_tasks": 10, "total_records": 1000}
    
    class MockTransformer:
        def transform(self):
            return True
    
    class MockLoader:
        def load(self, schemas=None, max_retries=3):
            # Mock loader implementation for testing
            return {"export": {"total_files": 5}, "upload": {"files_uploaded": 5}, "success": True}
    
    container.register_extractor("default", lambda **kwargs: MockExtractor(**kwargs))
    container.register_transformer("default", lambda: MockTransformer())
    container.register_loader("default", lambda: MockLoader())
    
    return container


# Global container instance (can be replaced for testing)
_container = None

def get_container() -> DependencyContainer:
    """Get the global dependency container."""
    global _container
    if _container is None:
        _container = create_default_container()
    return _container

def set_container(container: DependencyContainer):
    """Set the global dependency container (useful for testing)."""
    global _container
    _container = container

def get_cli_services() -> CLIServices:
    """Get CLI services with dependency injection."""
    return CLIServices(get_container())