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

def lambda_handler(event, context):
    # Retrieve AWS credentials from Secrets Manager
    ACCESS_KEY = get_secret('your_secret')['ACCESS_KEY']
    SECRET_KEY = get_secret('your_secret')['SECRET_KEY']

    # Initialize DynamoDB resource
    dynamodb = boto3.resource(
        'dynamodb',
        region_name='us-east-1',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

    # Specify the DynamoDB table names 
    table_name_messages = "telegram_messages" 
    messages_table = dynamodb.Table(table_name_messages)
    table_name_workouts = "telegram_workout_tracker" 
    workouts_table = dynamodb.Table(table_name_workouts)

    # Load exercise config 
    exercises = load_json_file(filename='workouts.json')
    exercise_options = [key for i in exercises.keys() for key in exercises[i].keys()]

    # Load sets & reps config 
    sets_reps = load_json_file(filename='sets_reps.json')
    sets_reps_names = sets_reps['workout_reps_sets'].keys()

    # Parse the incoming event from Telegram
    body = json.loads(event['body'])
    print(body)
    print("*** Received event")

    chat_id = body['message']['chat']['id']
    user_name = body['message']['from']['username']
    message_text = body['message']['text']
    message_id = body['message']['message_id']
    timestamp = body['message']['date']  # Unix timestamp of the message

    print(f"*** chat id: {chat_id}")
    print(f"*** message id: {message_id}")
    print(f"*** user name: {user_name}")
    print(f"*** message text: {message_text}")
    print(json.dumps(body))

    # Retrieve data from dynamo db 
    workouts_query_response = query_table(workouts_table,chat_id)
    workouts = unify_weights(transform_time_columns(pd.DataFrame(workouts_query_response)))

    # Start workflow and choose workout type 
    if message_text.lower() in ('/workout','/workouts'):
        send_reply(chat_id, 'Lets start the workout')
        # Reset state and start the workflow
        reset_user_state(chat_id,timestamp, workouts_table)
        message = 'Choose a workout type'
        send_response_keyboards(chat_id, message, exercises)
    # Enter exercise 
    elif message_text in exercises.keys():
        update_workout_type(workouts_table, chat_id, message_text, column='workout_type')
        message = 'Enter the exercise'
        send_response_keyboards(chat_id, message, exercises[message_text])
    # Enter sets & reps
    elif message_text in exercise_options:
        update_workout_type(workouts_table, chat_id, message_text, column='exercise')
        if message_text in ('Running', 'Bicycle'):
            message = 'Enter duration (min) - distance (km)'
            send_reply(chat_id, message)
        else:
            message = 'Enter the sets and reps'
            send_response_keyboards(chat_id, message, sets_reps['workout_reps_sets'])
    # Enter weight
    elif message_text in sets_reps['workout_reps_sets'].keys():
        update_workout_type(workouts_table, chat_id, message_text, column='sets_reps')
        message = 'Insert the weight in Kg or lbs'
        send_reply(chat_id, message)
    # Enter completed or failed
    elif validate_weight_input(message_text)['valid'] == True:
        update_workout_type(workouts_table, chat_id, validate_weight_input(message_text)['weight'], column='weight')
        update_workout_type(workouts_table, chat_id, validate_weight_input(message_text)['unit'], column='weight_unit')
        message = 'Did you complete the exercise?'
        send_response_keyboards(chat_id, message, sets_reps['workout_complete_fail'])
        # Store cardio 
        # Enter completed or failed
    elif validate_cardio_input(message_text)['valid'] == True:
        update_workout_type(workouts_table, chat_id, validate_cardio_input(message_text)['duration'], column='duration_min')
        update_workout_type(workouts_table, chat_id, validate_cardio_input(message_text)['distance'], column='distance_km')
        message = 'Did you complete the exercise?'
        send_response_keyboards(chat_id, message, sets_reps['workout_complete_fail'])
        # Start new entry
    elif message_text in sets_reps['workout_complete_fail'].keys():
        update_workout_type(workouts_table, chat_id, sets_reps['workout_complete_fail'][message_text], column='completed')
        message = (f"You {message_text.lower()} the Exercise. Start a new exercise with /workout \n")
        send_reply(chat_id, message)
        send_reply(chat_id, generate_analytics_message())

    elif message_text.lower() in ('/max_weights', '/max_weights_pull', '/max_weights_push','/max_weights_legs'):
        if message_text.lower() == '/max_weights_pull':
            workouts = workouts[workouts['workout_type'] == 'Pull']
        elif message_text.lower() == '/max_weights_push':
            workouts = workouts[workouts['workout_type'] == 'Push']
        elif message_text.lower() == '/max_weights_legs':
            workouts = workouts[workouts['workout_type'] == 'Legs']
        max_workout_weights = get_max_workout_weights(workouts)
        message = format_workout_message(max_workout_weights)
        send_reply(chat_id,message)
        send_reply(chat_id, generate_analytics_message())
        
    elif message_text.lower() in ('/last_workouts', '/last_workout'):
        message =  generate_last_workout_message(get_latest_workout_type(workouts))
        send_reply(chat_id, message)
    return {
        'statusCode': 200,
        'body': json.dumps('Message processed and stored successfully')
    }