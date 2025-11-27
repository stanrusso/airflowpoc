"""
Generic API client for handling various API connections.
Supports REST APIs with different authentication methods.
"""
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import requests
from pydantic import BaseModel, Field, field_validator, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class AuthType(str, Enum):
    """Supported authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC = "basic"
    OAUTH2 = "oauth2"
    CUSTOM_HEADER = "custom_header"


class HttpMethod(str, Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


# =============================================================================
# Pydantic Models for API Configuration
# =============================================================================

class APIAuthConfig(BaseModel):
    """Authentication configuration for API connections."""
    auth_type: AuthType = AuthType.NONE
    api_key: Optional[SecretStr] = None
    api_key_header: str = "X-API-Key"
    api_key_param: Optional[str] = None  # For query param auth
    bearer_token: Optional[SecretStr] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    oauth2_token_url: Optional[str] = None
    oauth2_client_id: Optional[str] = None
    oauth2_client_secret: Optional[SecretStr] = None
    oauth2_scope: Optional[str] = None
    custom_headers: Dict[str, str] = Field(default_factory=dict)
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Generate authentication headers based on auth type."""
        headers = {}
        
        if self.auth_type == AuthType.API_KEY and self.api_key:
            headers[self.api_key_header] = self.api_key.get_secret_value()
        elif self.auth_type == AuthType.BEARER_TOKEN and self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token.get_secret_value()}"
        elif self.auth_type == AuthType.CUSTOM_HEADER:
            headers.update(self.custom_headers)
            
        return headers
    
    def get_auth_params(self) -> Dict[str, str]:
        """Generate authentication query parameters."""
        params = {}
        if self.auth_type == AuthType.API_KEY and self.api_key and self.api_key_param:
            params[self.api_key_param] = self.api_key.get_secret_value()
        return params
    
    def get_basic_auth(self) -> Optional[tuple]:
        """Get basic auth tuple if applicable."""
        if self.auth_type == AuthType.BASIC and self.username and self.password:
            return (self.username, self.password.get_secret_value())
        return None


class APIEndpointConfig(BaseModel):
    """Configuration for a specific API endpoint."""
    name: str = Field(..., description="Unique identifier for this endpoint")
    path: str = Field(..., description="API endpoint path (appended to base_url)")
    method: HttpMethod = HttpMethod.GET
    description: str = ""
    
    # Request configuration
    params: Dict[str, Any] = Field(default_factory=dict)
    headers: Dict[str, str] = Field(default_factory=dict)
    body: Optional[Dict[str, Any]] = None
    
    # Pagination
    paginated: bool = False
    page_param: str = "page"
    page_size_param: str = "page_size"
    page_size: int = 100
    max_pages: Optional[int] = None
    
    # Response parsing
    data_key: Optional[str] = None  # JSON key containing the data array
    next_page_key: Optional[str] = None  # JSON key for next page indicator
    
    # Rate limiting
    rate_limit_requests: Optional[int] = None
    rate_limit_period_seconds: int = 60


class APISourceConfig(BaseModel):
    """Complete configuration for an API data source."""
    name: str = Field(..., description="Unique name for this API source")
    base_url: str = Field(..., description="Base URL for the API")
    auth: APIAuthConfig = Field(default_factory=APIAuthConfig)
    
    # Default settings
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    verify_ssl: bool = True
    
    # Default headers for all requests
    default_headers: Dict[str, str] = Field(default_factory=lambda: {
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    
    # Endpoints
    endpoints: Dict[str, APIEndpointConfig] = Field(default_factory=dict)
    
    # Airflow connection (optional - for using Airflow's connection management)
    airflow_conn_id: Optional[str] = None
    
    def get_endpoint(self, endpoint_name: str) -> APIEndpointConfig:
        """Get endpoint configuration by name."""
        if endpoint_name not in self.endpoints:
            raise ValueError(f"Endpoint '{endpoint_name}' not found. Available: {list(self.endpoints.keys())}")
        return self.endpoints[endpoint_name]
    
    def add_endpoint(self, endpoint: APIEndpointConfig) -> "APISourceConfig":
        """Add an endpoint to this source."""
        self.endpoints[endpoint.name] = endpoint
        return self


# =============================================================================
# API Client Implementation
# =============================================================================

class APIClient:
    """
    Generic API client for making HTTP requests.
    
    Usage:
        config = APISourceConfig(
            name="my_api",
            base_url="https://api.example.com",
            auth=APIAuthConfig(
                auth_type=AuthType.BEARER_TOKEN,
                bearer_token="my-token"
            )
        )
        
        client = APIClient(config)
        data = client.fetch("users")
    """
    
    def __init__(self, config: APISourceConfig):
        self.config = config
        self.session = requests.Session()
        self._setup_session()
    
    def _setup_session(self):
        """Configure the session with default settings."""
        self.session.headers.update(self.config.default_headers)
        self.session.headers.update(self.config.auth.get_auth_headers())
        self.session.verify = self.config.verify_ssl
        
        # Set up retries
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_seconds,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def _build_url(self, endpoint: APIEndpointConfig) -> str:
        """Build full URL from base URL and endpoint path."""
        base = self.config.base_url.rstrip("/")
        path = endpoint.path.lstrip("/")
        return f"{base}/{path}"
    
    def _make_request(
        self,
        endpoint: APIEndpointConfig,
        extra_params: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        """Make a single HTTP request."""
        url = self._build_url(endpoint)
        
        # Merge parameters
        params = {**endpoint.params, **(extra_params or {})}
        params.update(self.config.auth.get_auth_params())
        
        # Merge headers
        headers = {**endpoint.headers, **(extra_headers or {})}
        
        # Get basic auth if applicable
        auth = self.config.auth.get_basic_auth()
        
        logger.info(f"Making {endpoint.method.value} request to {url}")
        
        response = self.session.request(
            method=endpoint.method.value,
            url=url,
            params=params,
            headers=headers,
            json=endpoint.body,
            auth=auth,
            timeout=self.config.timeout_seconds,
        )
        
        response.raise_for_status()
        return response
    
    def fetch(
        self,
        endpoint_name: str,
        extra_params: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch data from an endpoint, handling pagination if configured.
        
        Args:
            endpoint_name: Name of the endpoint to fetch from
            extra_params: Additional query parameters
            extra_headers: Additional headers
            
        Returns:
            List of records from the API
        """
        endpoint = self.config.get_endpoint(endpoint_name)
        all_data = []
        
        if endpoint.paginated:
            all_data = self._fetch_paginated(endpoint, extra_params, extra_headers)
        else:
            response = self._make_request(endpoint, extra_params, extra_headers)
            data = response.json()
            
            # Extract data from nested key if specified
            if endpoint.data_key:
                data = data.get(endpoint.data_key, [])
            
            all_data = data if isinstance(data, list) else [data]
        
        logger.info(f"Fetched {len(all_data)} records from {endpoint_name}")
        return all_data
    
    def _fetch_paginated(
        self,
        endpoint: APIEndpointConfig,
        extra_params: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all pages from a paginated endpoint."""
        all_data = []
        page = 1
        
        while True:
            # Add pagination parameters
            page_params = {
                endpoint.page_param: page,
                endpoint.page_size_param: endpoint.page_size,
                **(extra_params or {}),
            }
            
            response = self._make_request(endpoint, page_params, extra_headers)
            data = response.json()
            
            # Extract data from nested key if specified
            if endpoint.data_key:
                page_data = data.get(endpoint.data_key, [])
            else:
                page_data = data if isinstance(data, list) else [data]
            
            if not page_data:
                break
            
            all_data.extend(page_data)
            logger.info(f"Fetched page {page}, total records: {len(all_data)}")
            
            # Check for next page
            if endpoint.next_page_key:
                if not data.get(endpoint.next_page_key):
                    break
            elif len(page_data) < endpoint.page_size:
                break
            
            # Check max pages limit
            if endpoint.max_pages and page >= endpoint.max_pages:
                logger.warning(f"Reached max pages limit: {endpoint.max_pages}")
                break
            
            page += 1
        
        return all_data
    
    def fetch_to_dataframe(
        self,
        endpoint_name: str,
        extra_params: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ):
        """Fetch data and return as a pandas DataFrame."""
        import pandas as pd
        
        data = self.fetch(endpoint_name, extra_params, extra_headers)
        return pd.DataFrame(data)
    
    def close(self):
        """Close the session."""
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# =============================================================================
# Factory Functions for Common API Patterns
# =============================================================================

def create_rest_api_source(
    name: str,
    base_url: str,
    auth_type: AuthType = AuthType.NONE,
    api_key: Optional[str] = None,
    bearer_token: Optional[str] = None,
    **kwargs
) -> APISourceConfig:
    """
    Factory function to create common REST API configurations.
    
    Usage:
        source = create_rest_api_source(
            name="github",
            base_url="https://api.github.com",
            auth_type=AuthType.BEARER_TOKEN,
            bearer_token="ghp_xxx"
        )
    """
    auth_config = APIAuthConfig(auth_type=auth_type)
    
    if api_key:
        auth_config.api_key = SecretStr(api_key)
    if bearer_token:
        auth_config.bearer_token = SecretStr(bearer_token)
    
    return APISourceConfig(
        name=name,
        base_url=base_url,
        auth=auth_config,
        **kwargs
    )
