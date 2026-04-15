"""
Salesforce authentication module
Python equivalent of sf_auth.mjs
"""

import os
import json
import requests
from .secret_manager import get_secret
from .sf_utils import validate_oauth_config


# Environment variables
SF_TOKEN_HOST = os.environ.get('SF_TOKEN_HOST', 'qcellsnorthamerica123--qa.sandbox.my.salesforce.com')
SF_TOKEN_PATH = os.environ.get('SF_TOKEN_PATH', '/services/oauth2/token')
SECRET_NAME = os.environ.get('SECRET_NAME', 'xq-sf-token')
REGION_NAME = os.environ.get('REGION_NAME', 'us-west-2')


def get_access_token():
    """
    Get Salesforce access token using OAuth
    Python equivalent of getAccessToken() in sf_auth.mjs
    
    Returns:
        Dictionary with access_token and instance_url
        
    Raises:
        Exception if authentication fails
    """
    # Get secret from AWS Secrets Manager
    secret_res = get_secret(SECRET_NAME, REGION_NAME)
    
    if not secret_res:
        raise Exception("Unable to retrieve Salesforce secret")
    
    # Validate required fields
    if not all(key in secret_res for key in ['client_id', 'client_secret', 'grant_type']):
        raise Exception("Salesforce secret missing required fields (client_id, client_secret, grant_type)")
    
    # Validate OAuth configuration
    validate_oauth_config(secret_res)
    
    # Build token request URL
    token_url = f"https://{SF_TOKEN_HOST}{SF_TOKEN_PATH}"
    
    # Prepare request body (form-encoded)
    body = {
        'grant_type': secret_res['grant_type'],
        'client_id': secret_res['client_id'],
        'client_secret': secret_res['client_secret']
    }
    
    # Add username/password if using password grant type
    if secret_res['grant_type'] == 'password':
        body['username'] = secret_res.get('username')
        body['password'] = secret_res.get('password')
    
    # Make token request
    try:
        response = requests.post(
            token_url,
            data=body,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        # Parse response
        parsed = response.json()
        
        if response.status_code != 200:
            error_msg = parsed.get('error_description') or f"Token request failed with status {response.status_code}"
            raise Exception(error_msg)
        
        if 'access_token' not in parsed or 'instance_url' not in parsed:
            raise Exception("Token response missing access_token or instance_url")
        
        return {
            'access_token': parsed['access_token'],
            'instance_url': parsed['instance_url']
        }
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Token request failed: {str(e)}")


def sf_query(query):
    """
    Execute SOQL query against Salesforce
    Python equivalent of sfQuery() in sf_auth.mjs
    
    Args:
        query: SOQL query string
        
    Returns:
        Dictionary with 'data' (query results) and 'instance_url'
        
    Raises:
        Exception if query fails
    """
    if not query or not isinstance(query, str):
        raise ValueError("SOQL query must be a non-empty string")
    
    # Get access token
    token_res = get_access_token()
    
    if 'access_token' not in token_res or 'instance_url' not in token_res:
        raise Exception("Missing Salesforce access context")
    
    # Build query URL
    query_url = f"{token_res['instance_url']}/services/data/v59.0/query/"
    
    print(f"Salesforce SOQL query: {query}")
    
    # Make query request
    try:
        response = requests.get(
            query_url,
            params={'q': query},
            headers={
                'Authorization': f"Bearer {token_res['access_token']}",
                'Content-Type': 'application/json'
            }
        )
        
        print(f"Salesforce SOQL HTTP response status: {response.status_code}")
        
        data = response.json()
        
        if response.status_code != 200:
            error_msg = data.get('message') or f"SOQL query failed with status {response.status_code}"
            raise Exception(error_msg)
        
        return {
            'data': data,
            'instance_url': token_res['instance_url']
        }
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"SOQL query request failed: {str(e)}")

