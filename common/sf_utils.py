"""
Salesforce utilities module
Python equivalent of sf_utils.mjs
"""

import json
import urllib.parse


def validate_oauth_config(config):
    """
    Validate OAuth configuration
    
    Args:
        config: Dictionary with OAuth configuration
        
    Raises:
        ValueError if configuration is invalid
    """
    required_fields = ['client_id', 'client_secret', 'grant_type']
    
    for field in required_fields:
        if field not in config or not config[field]:
            raise ValueError(f"OAuth config missing required field: {field}")
    
    # Validate grant_type
    valid_grant_types = ['password', 'client_credentials', 'authorization_code', 'refresh_token']
    if config['grant_type'] not in valid_grant_types:
        raise ValueError(f"Invalid grant_type: {config['grant_type']}")
    
    # For password grant type, username and password are required
    if config['grant_type'] == 'password':
        if 'username' not in config or not config['username']:
            raise ValueError("Username is required for password grant type")
        if 'password' not in config or not config['password']:
            raise ValueError("Password is required for password grant type")
    
    return True


def safe_json(text):
    """
    Safely parse JSON string
    
    Args:
        text: String to parse as JSON
        
    Returns:
        Parsed JSON object or None if parsing fails
    """
    if not text:
        return None
    
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def build_query_string(params):
    """
    Build URL query string from parameters
    
    Args:
        params: Dictionary of query parameters
        
    Returns:
        URL-encoded query string
    """
    if not params:
        return ''
    
    return urllib.parse.urlencode(params)


def encode_soql(query):
    """
    URL-encode SOQL query
    
    Args:
        query: SOQL query string
        
    Returns:
        URL-encoded query string
    """
    if not query:
        return ''
    
    return urllib.parse.quote(query, safe='')

