from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from datetime import datetime 
import json
import urllib3
import boto3
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import os
import pandas as pd 
import numpy as np
from decimal import Decimal, InvalidOperation
from analytics import * 
from utils import *
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Initialize AWS Clients Outside the Handler for Reuse
REGION_NAME = os.getenv('AWS_REGION', 'us-east-1')
SECRETS_CACHE = {}

secrets_client = boto3.client('secretsmanager', region_name=REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)

def load_json_file(filename):
    """
    Loads a JSON file and returns its contents.

    Args:
        filename (str): The name of the JSON file.

    Returns:
        dict: The contents of the JSON file.
    """
    try:
        with open(filename, 'r') as json_file:
            return json.load(json_file)
    except Exception as e:
        logger.error(f"Error loading JSON file {filename}: {e}")
        raise


def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager and caches it for future use.

    Args:
        secret_name (str): The name of the secret to retrieve.

    Returns:
        dict: The secret as a dictionary.
    """
    if secret_name in SECRETS_CACHE:
        return SECRETS_CACHE[secret_name]

    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        SECRETS_CACHE[secret_name] = secret
        return secret
    except ClientError as e:
        logger.error(f"Error retrieving secret {secret_name}: {e}")
        raise

def convert_timestamp_to_date(timestamp):
    """
    Converts a Unix timestamp to a date in YYYY-MM-DD format.

    Args:
        timestamp (int): The Unix timestamp to convert.

    Returns:
        str: The date in YYYY-MM-DD format.
    """
    # Convert the timestamp to a datetime object
    dt_object = datetime.utcfromtimestamp(timestamp)

    # Format the datetime object to YYYY-MM-DD
    formatted_date = dt_object.strftime('%Y-%m-%d')

    return formatted_date


def send_reply(chat_id, message, reply_markup=None):
    """
    Sends a reply message to the specified Telegram chat.

    Args:
        chat_id (int): The Telegram chat ID.
        message (str): The message to send.
        reply_markup (dict, optional): The reply markup for custom keyboards.
    """
    BOT_TOKEN = get_secret('telegram_bot_token_1')['token']
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message
    }

    if reply_markup:
        payload["reply_markup"] = reply_markup

    http = urllib3.PoolManager()
    encoded_data = json.dumps(payload).encode('utf-8')
    response = http.request(
        'POST',
        url,
        body=encoded_data,
        headers={'Content-Type': 'application/json'}
    )

    if response.status != 200:
        logger.error(f"Failed to send message to chat_id {chat_id}: {response.data}")
    else:
        logger.info(f"Sent message to chat_id {chat_id}: {message}")

def reset_user_state(chat_id, timestamp, table):
    """
    Resets the user's state in the DynamoDB table.

    Args:
        chat_id (int): The chat ID.
        timestamp (int): The timestamp of the reset.
        table (Table): The DynamoDB table object.
    """
    try:
        table.put_item(
            Item={
                'chat_id': int(chat_id),
                'timestamp': int(timestamp),
                'workout_date': convert_timestamp_to_date(timestamp)
            }
        )
        logger.info(f"User state reset for chat_id {chat_id}")
    except ClientError as e:
        logger.error(f"Error resetting user state for chat_id {chat_id}: {e}")
        raise

def retrieve_latest_record(table, chat_id):
    """
    Retrieves the latest workout record for a given chat_id.

    Args:
        table (Table): The DynamoDB table object.
        chat_id (int): The chat ID.

    Returns:
        dict or None: The latest record or None if not found.
    """
    try:
        response = table.query(
            KeyConditionExpression=Key('chat_id').eq(chat_id),
            ScanIndexForward=False,  
            Limit=1
        )

        if 'Items' in response and response['Items']:
            latest_record = response['Items'][0]
            logger.debug(f"Latest Record for chat_id {chat_id}: {latest_record}")
            return latest_record
        else:
            logger.info(f"No records found for chat_id: {chat_id}")
            return None
    except Exception as e:
        logger.error(f"Error retrieving latest record for chat_id {chat_id}: {e}")
        return None

def update_workout_type(table, chat_id, workout_type_value, column):
    """
    Updates a specific column for the latest workout record of a chat_id.

    Args:
        table (Table): The DynamoDB table object.
        chat_id (int): The chat_id to filter records by.
        workout_type_value: The value to set for the specified column.
        column (str): The column to update.

    Returns:
        bool: True if update was successful, False otherwise.
    """
    try:
        latest_record = retrieve_latest_record(table, chat_id)
        if not latest_record:
            logger.warning(f"No records found for chat_id: {chat_id}")
            return False

        table.update_item(
            Key={
                'chat_id': latest_record['chat_id'],
                'timestamp': latest_record['timestamp']
            },
            UpdateExpression=f"SET {column} = :value",
            ExpressionAttributeValues={
                ':value': workout_type_value
            }
        )
        logger.info(f"Updated {column} for chat_id {chat_id} to {workout_type_value}")
        return True
    except Exception as e:
        logger.error(f"Error updating {column} for chat_id {chat_id}: {e}")
        return False



def send_response_keyboards(chat_id, message, options_dict):
    """
    Sends a reply message with dynamically generated custom keyboards to the Telegram chat.

    Args:
        chat_id (int): The Telegram chat ID.
        message (str): The message to send.
        options_dict (dict): A dictionary with keys as button labels.
    """
    keyboard = [[key] for key in options_dict.keys()]
    reply_markup = {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": True
    }
    send_reply(chat_id, message, reply_markup)


import re

def validate_weight_input(message):
    """
    Validates if the input message matches the format of a number followed by a weight unit (kg or lbs).

    Args:
        message (str): The input message to validate.

    Returns:
        dict: A dictionary with validation status and extracted weight and unit if valid.
    """
    # Define the regex pattern for weight input
    pattern = r"^\s*(\d+(\.\d+)?)\s*(kg|lbs)\s*$"  # Matches numbers with optional decimals followed by 'kg' or 'lbs'

    # Match the input message against the pattern
    match = re.match(pattern, message, re.IGNORECASE)

    if match:
        # Extract the weight and unit from the match
        weight = match.group(1)  # Convert the weight to a float
        unit = match.group(3).lower()  # Extract the unit and convert to lowercase
        print(weight, unit)
        return {"valid": True, "weight": weight, "unit": unit}
    else:
        return {"valid": False, "error": "Invalid format. Please enter weight as '<number> kg' or '<number> lbs'."}

def validate_cardio_input(message):
    """
    Validates if the input message matches the format of duration followed by distance (e.g., '25 min - 8 km').

    Args:
        message (str): The input message to validate.

    Returns:
        dict: A dictionary with validation status and extracted duration and distance if valid.
    """
    # Define the regex pattern for cardio input
    pattern = r"^\s*(\d+)\s*min\s*-\s*(\d+(?:[.,]\d+)?)\s*km\s*$"  # Matches 'number min - number km'

    # Match the input message against the pattern
    match = re.match(pattern, message, re.IGNORECASE)

    if match:
        try:
            # Extract the duration and distance from the match
            duration = int(match.group(1))  # Convert the duration to an integer
            distance = match.group(2).replace(',', '.')  # Replace comma with dot for decimal conversion
            
            # Convert distance to Decimal
            distance = Decimal(distance)
            
            return {"valid": True, "duration": duration, "distance": distance}
        except (ValueError, InvalidOperation) as e:
            # Handle invalid decimal conversion
            return {"valid": False, "error": f"Invalid distance value: {e}"}
    else:
        return {"valid": False, "error": "Invalid format. Please enter cardio input as '<number> min - <number> km'."}

