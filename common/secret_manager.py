"""
AWS Secrets Manager utility module
Python equivalent of secret_manager.mjs
"""

import json
import boto3
from botocore.exceptions import ClientError


def get_secret(secret_name, region_name):
    """
    Retrieve secret from AWS Secrets Manager
    
    Args:
        secret_name: Name of the secret in Secrets Manager
        region_name: AWS region name
        
    Returns:
        Dictionary with secret values
        
    Raises:
        Exception if secret cannot be retrieved
    """
    if not secret_name:
        raise ValueError("Secret name is required")
    
    if not region_name:
        raise ValueError("Region name is required")
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'DecryptionFailureException':
            raise Exception(f"Secrets Manager can't decrypt the secret: {str(e)}")
        elif error_code == 'InternalServiceErrorException':
            raise Exception(f"Internal service error: {str(e)}")
        elif error_code == 'InvalidParameterException':
            raise Exception(f"Invalid parameter: {str(e)}")
        elif error_code == 'InvalidRequestException':
            raise Exception(f"Invalid request: {str(e)}")
        elif error_code == 'ResourceNotFoundException':
            raise Exception(f"Secret '{secret_name}' not found in region '{region_name}'")
        else:
            raise Exception(f"Error retrieving secret: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error retrieving secret: {str(e)}")
    
    # Parse and return the secret
    if 'SecretString' in get_secret_value_response:
        secret_string = get_secret_value_response['SecretString']
        try:
            secret_dict = json.loads(secret_string)
            return secret_dict
        except json.JSONDecodeError:
            # If it's not JSON, return as plain string in a dict
            return {'value': secret_string}
    else:
        # Binary secret (not typically used for Salesforce credentials)
        raise Exception("Binary secrets are not supported")

